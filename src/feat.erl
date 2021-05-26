-module(feat).

-include("feat.hrl").

-type request_key() :: binary().
-type request_value() :: integer() | binary() | request() | [request()].
-type request() :: #{request_key() := request_value()}.

-type feature_name() :: integer().
-type feature_value() :: integer() | features() | [feature_value()] | undefined.
-type features() :: #{feature_name() := feature_value()}.
-type accessor() :: request_key() | nonempty_list(request_key()).
-type request_schema() :: schema() | {set, schema()}.
-type schema() ::
    #{
        feature_name() := accessor() | {accessor(), request_schema()} | 'reserved'
    }
    | #{
        ?discriminator := accessor(),
        feature_name() := schema()
    }.
-type difference() :: ?difference | #{feature_name() := difference()}.

-type event() ::
    {invalid_schema_fragment, feature_name(), request()}
    | {request_visited, {request, request()}}
    | {request_key_index_visit, integer()}
    | {request_key_index_visited, integer()}
    | {request_key_visit, {key, integer(), request()}}
    | {request_key_visited, {key, integer()}}.

-type event_handler() :: {module(), options()} | undefined.
-type options() :: term().

-export_type([request_key/0]).
-export_type([request_value/0]).
-export_type([request/0]).
-export_type([feature_name/0]).
-export_type([feature_value/0]).
-export_type([features/0]).
-export_type([schema/0]).
-export_type([difference/0]).
-export_type([event/0]).
-export_type([event_handler/0]).
-export_type([options/0]).

-export([read/2, read/3]).
-export([compare/2]).
-export([hash/1]).
-export([list_diff_fields/2]).

-callback handle_event(event(), options()) -> ok.

-spec read(schema(), request()) -> features().
read(Schema, Request) ->
    read(get_event_handler(), Schema, Request).

-spec read(event_handler(), schema(), request()) -> features().
read(Handler, Schema, Request) ->
    handle_event(get_event_handler(Handler), {request_visited, {request, Request}}),
    read_(Schema, Request, Handler).

read_(_, undefined, _Handler) ->
    undefined;
read_({set, Schema}, RequestList, Handler) when is_map(Schema) and is_list(RequestList) ->
    {_, ListIndex} = lists:foldl(fun(Item, {N, Acc}) -> {N + 1, [{N, Item} | Acc]} end, {0, []}, RequestList),

    ListSorted = lists:keysort(2, ListIndex),
    lists:foldl(
        fun({Index, Req}, Acc) ->
            handle_event(get_event_handler(Handler), {request_key_index_visit, Index}),
            Value = read_(Schema, Req, Handler),
            handle_event(get_event_handler(Handler), {request_key_index_visited, Index}),
            [[Index, Value] | Acc]
        end,
        [],
        ListSorted
    );
read_(UnionSchema = #{?discriminator := DiscriminatorAccessor}, Request, Handler) ->
    DiscriminatorValue =
        read_hashed_request_value(
            wrap_accessor(DiscriminatorAccessor),
            Request,
            Handler
        ),

    maps:fold(
        fun(Name, Schema, Acc) ->
            VariantValue = read_(Schema, Request, Handler),
            Acc#{Name => VariantValue}
        end,
        #{?discriminator => DiscriminatorValue},
        maps:remove(?discriminator, UnionSchema)
    );
read_(Schema, Request, Handler) when is_map(Schema) ->
    maps:fold(
        fun
            (_Name, 'reserved', Acc) ->
                Acc;
            (Name, {Accessor, NestedSchema}, Acc) ->
                AccessorList = wrap_accessor(Accessor),
                NestedRequest = read_raw_request_value(AccessorList, Request, Handler),
                Value = read_(NestedSchema, NestedRequest, Handler),
                Acc#{Name => Value};
            (Name, Accessor, Acc) ->
                AccessorList = wrap_accessor(Accessor),
                FeatureValue = read_hashed_request_value(AccessorList, Request, Handler),
                Acc#{Name => FeatureValue}
        end,
        #{},
        Schema
    ).

read_raw_request_value(Accessor, Request, Handler) ->
    case read_request_value(Accessor, Request, Handler) of
        {ok, Value} -> Value;
        undefined -> undefined
    end.
read_hashed_request_value(Accessor, Request, Handler) ->
    case read_request_value(Accessor, Request, Handler) of
        {ok, Value} -> hash(Value);
        undefined -> undefined
    end.

read_request_value([], Value, _) ->
    {ok, Value};
read_request_value([Key | Rest], Request = #{}, Handler) when is_binary(Key) ->
    case maps:find(Key, Request) of
        {ok, SubRequest} ->
            handle_event(get_event_handler(Handler), {request_key_visit, {key, Key, SubRequest}}),
            Result = read_request_value(Rest, SubRequest, Handler),
            handle_event(get_event_handler(Handler), {request_key_visited, {key, Key}}),
            Result;
        error ->
            undefined
    end;
read_request_value(_, undefined, _) ->
    undefined;
read_request_value(Key, Request, Handler) ->
    handle_event(get_event_handler(Handler), {invalid_schema_fragment, Key, Request}).

handle_event(undefined, {invalid_schema_fragment, Key, Request}) ->
    logger:warning("Unable to extract idemp feature with schema: ~p from client request subset: ~p", [Key, Request]),
    undefined;
handle_event(undefined, _Event) ->
    ok;
handle_event({Mod, Opts}, Event) ->
    Mod:handle_event(Event, Opts).

get_event_handler() ->
    genlib_app:env(feat, event_handler, undefined).

get_event_handler({Mod, Options}) ->
    {Mod, Options};
get_event_handler(undefined) ->
    undefined.

-spec hash(term()) -> integer().
hash(V) ->
    erlang:phash2(V).

-spec list_diff_fields(schema(), difference()) -> all | [binary()].
list_diff_fields(_Schema, ?difference) ->
    all;
list_diff_fields(Schema, Diff) ->
    {ConvertedDiff, _} = list_diff_fields_(Diff, Schema, {[], []}),
    lists:foldl(
        fun(Keys, AccIn) ->
            KeysBin = lists:map(fun genlib:to_binary/1, Keys),
            Item = list_to_binary(lists:join(<<".">>, KeysBin)),
            case lists:member(Item, AccIn) of
                false ->
                    [Item | AccIn];
                _ ->
                    AccIn
            end
        end,
        [],
        ConvertedDiff
    ).

list_diff_fields_(Diffs, {set, Schema}, Acc) when is_map(Schema) ->
    maps:fold(
        fun
            (I, ?difference, {PathsAcc, PathRev}) ->
                Path = lists:reverse([I | PathRev]),
                {[Path | PathsAcc], PathRev};
            (I, Diff, {PathsAcc, PathRev}) ->
                {NewPathsAcc, _NewPathRev} = list_diff_fields_(Diff, Schema, {PathsAcc, [I | PathRev]}),
                {NewPathsAcc, PathRev}
        end,
        Acc,
        Diffs
    );
%% If discriminator is different, diff would've been minimized because of it:
%% => implying, discriminator is not different here
list_diff_fields_(Diff, Schema = #{?discriminator := _}, Acc) ->
    zipfold(
        fun
            (Variant, ?difference, _Schema, {PathsAcc, PathRev}) ->
                {[lists:reverse([Variant | PathRev]) | PathsAcc], PathRev};
            (_Variant, VariantDiff, VariantSchema, {_PathsAcc, PathRev} = AccIn) ->
                {NewPathsAcc, _NewPathRev} = list_diff_fields_(VariantDiff, VariantSchema, AccIn),
                {NewPathsAcc, PathRev}
        end,
        Acc,
        Diff,
        Schema
    );
list_diff_fields_(Diff, Schema, Acc) when is_map(Schema) ->
    zipfold(
        fun
            (_Feature, ?difference, SchemaPart, {PathsAcc, PathRev}) ->
                Path = lists:reverse(PathRev) ++ get_path(SchemaPart),
                {[Path | PathsAcc], PathRev};
            (_Feature, DiffPart, SchemaPart, {_PathsAcc, PathRev} = AccIn) ->
                {NewPathsAcc, _NewPathRev} = list_diff_fields_(DiffPart, SchemaPart, AccIn),
                {NewPathsAcc, PathRev}
        end,
        Acc,
        Diff,
        Schema
    );
list_diff_fields_(Diff, {Accessor, Schema}, {PathsAcc, PathRev}) ->
    Path = accessor_to_path(Accessor),
    list_diff_fields_(Diff, Schema, {PathsAcc, lists:reverse(Path) ++ PathRev}).

wrap_accessor(Accessor) ->
    if
        is_list(Accessor) ->
            Accessor;
        is_binary(Accessor) ->
            [Accessor]
    end.

get_path({Accessor, _Schema}) ->
    accessor_to_path(Accessor);
get_path(Accessor) ->
    accessor_to_path(Accessor).

accessor_to_path(Key) when is_binary(Key) ->
    [Key];
accessor_to_path(Keys) when is_list(Keys) ->
    Keys.

-spec compare(features(), features()) -> true | {false, difference()}.
compare(Features, FeaturesWith) ->
    case compare_features(Features, FeaturesWith) of
        ?difference ->
            {false, ?difference};
        Diff when map_size(Diff) > 0 ->
            {false, Diff};
        _ ->
            true
    end.

compare_features(Fs, FsWith) ->
    minimize_diff(
        zipfold(
            fun
                (Key, Values, ValuesWith, Diff) when is_list(ValuesWith), is_list(Values) ->
                    compare_list_features(Key, Values, ValuesWith, Diff);
                (Key, Value, ValueWith, Diff) when is_map(ValueWith) and is_map(Value) ->
                    Diff#{Key => compare_features(Value, ValueWith)};
                %% We expect that clients may _at any time_ change their implementation and start
                %% sending information they were not sending beforehand, so this is not considered a
                %% conflict. Yet, we DO NOT expect them to do the opposite, to stop sending
                %% information they were sending, this is still a conflict.
                (_Key, _Value, undefined, Diff) ->
                    Diff;
                (_Key, Value, Value, Diff) ->
                    Diff;
                (Key, Value, ValueWith, Diff) when Value =/= ValueWith ->
                    Diff#{Key => ?difference}
            end,
            #{},
            Fs,
            FsWith
        )
    ).

compare_list_features(Key, L1, L2, Diff) when length(L1) =/= length(L2) ->
    Diff#{Key => ?difference};
compare_list_features(Key, L1, L2, Acc) ->
    Acc#{Key => compare_list_features_(L1, L2, #{})}.

compare_list_features_([], [], Diff) ->
    minimize_diff(Diff);
compare_list_features_([[Index, V1] | Values], [[_, V2] | ValuesWith], DiffAcc) ->
    NewDiffAcc = DiffAcc#{Index => compare_features(V1, V2)},
    compare_list_features_(Values, ValuesWith, NewDiffAcc).

minimize_diff(?difference) ->
    ?difference;
minimize_diff(EmptyDiff) when map_size(EmptyDiff) == 0 ->
    #{};
%% Different with regard to discriminator, semantically same as different everywhere.
minimize_diff(#{?discriminator := _}) ->
    ?difference;
minimize_diff(Diff) ->
    %% CC = Complex diff Count
    {CC, TruncatedDiff} =
        maps:fold(
            fun(Key, NestedDiff, {CC, DiffAcc}) ->
                NewCC =
                    case {Key, NestedDiff} of
                        {?discriminator, _} -> CC;
                        {_, ?difference} -> CC;
                        {_, _} -> CC + 1
                    end,
                NewDiffAcc =
                    if
                        map_size(NestedDiff) == 0 ->
                            maps:remove(Key, DiffAcc);
                        true ->
                            DiffAcc
                    end,

                {NewCC, NewDiffAcc}
            end,
            {0, Diff},
            Diff
        ),
    if
        CC == 0, map_size(TruncatedDiff) /= 0 ->
            ?difference;
        true ->
            TruncatedDiff
    end.

zipfold(Fun, Acc, M1, M2) ->
    maps:fold(
        fun(Key, V1, AccIn) ->
            case maps:find(Key, M2) of
                {ok, V2} ->
                    Fun(Key, V1, V2, AccIn);
                error ->
                    AccIn
            end
        end,
        Acc,
        M1
    ).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

%% Serves as an example of the syntax
-define(SCHEMA, #{
    1 =>
        {<<"1">>,
            {set, #{
                ?discriminator => [<<"meta">>, <<"type">>],
                2 => #{
                    21 => <<"21">>,
                    22 => 'reserved'
                },
                3 => #{
                    31 => {<<"31">>, {set, #{311 => <<"311">>}}}
                },
                %% Tests correct list diff minimization
                4 => #{
                    41 =>
                        {<<"41">>, #{
                            411 => {<<"411">>, {set, #{}}},
                            412 => <<"412">>
                        }}
                }
            }}}
}).

-define(REQUEST, #{
    <<"1">> => [
        #{
            <<"meta">> => #{<<"type">> => <<"a">>},
            <<"21">> => <<"a_21">>,
            <<"unused">> => 42
        },
        #{
            <<"meta">> => #{<<"type">> => <<"b">>},
            <<"21">> => <<"b_21">>,
            <<"unused">> => 42
        },
        #{
            <<"meta">> => #{<<"type">> => <<"c">>},
            <<"31">> => [
                #{<<"311">> => <<"c_311_1">>},
                #{<<"311">> => <<"c_311_2">>}
            ]
        },
        #{
            <<"meta">> => #{<<"type">> => <<"d">>},
            <<"41">> => #{
                <<"411">> => [],
                <<"412">> => <<"d_412">>
            }
        },
        #{<<"meta">> => #{<<"type">> => <<"unchanged">>}}
    ]
}).

-define(OTHER_REQUEST, #{
    <<"1">> => [
        #{
            <<"meta">> => #{<<"type">> => <<"AAA">>},
            <<"21">> => <<"a_21">>,
            <<"unused">> => 43
        },
        #{
            <<"meta">> => #{<<"type">> => <<"b">>},
            <<"21">> => <<"b_21_other">>,
            <<"unused">> => 43
        },
        #{
            <<"meta">> => #{<<"type">> => <<"c">>},
            <<"31">> => [
                #{<<"311">> => <<"c_311_1_other">>},
                #{<<"311">> => <<"c_311_2">>}
            ]
        },
        #{
            <<"meta">> => #{<<"type">> => <<"d">>},
            <<"41">> => #{
                <<"411">> => [],
                <<"412">> => <<"d_412_other">>
            }
        },
        #{
            <<"meta">> => #{<<"type">> => <<"unchanged">>}
        }
    ]
}).

-spec simple_featurefull_schema_read_test() -> _.
simple_featurefull_schema_read_test() ->
    ?assertEqual(
        #{
            1 => [
                [
                    1,
                    #{
                        ?discriminator => hash(<<"b">>),
                        2 => #{21 => hash(<<"b_21">>)},
                        3 => #{31 => undefined},
                        4 => #{41 => undefined}
                    }
                ],
                [
                    0,
                    #{
                        ?discriminator => hash(<<"a">>),
                        2 => #{21 => hash(<<"a_21">>)},
                        3 => #{31 => undefined},
                        4 => #{41 => undefined}
                    }
                ],
                [
                    3,
                    #{
                        -1 => 109142861,
                        2 => #{21 => undefined},
                        3 => #{31 => undefined},
                        4 => #{41 => #{411 => [], 412 => hash(<<"d_412">>)}}
                    }
                ],

                [
                    2,
                    #{
                        ?discriminator => hash(<<"c">>),
                        2 => #{
                            21 => undefined
                        },
                        3 => #{
                            31 => [
                                [1, #{311 => hash(<<"c_311_2">>)}],
                                [0, #{311 => hash(<<"c_311_1">>)}]
                            ]
                        },
                        4 => #{41 => undefined}
                    }
                ],
                [
                    4,
                    #{
                        ?discriminator => hash(<<"unchanged">>),
                        2 => #{21 => undefined},
                        3 => #{31 => undefined},
                        4 => #{41 => undefined}
                    }
                ]
            ]
        },
        read(?SCHEMA, ?REQUEST)
    ).

-spec simple_featurefull_schema_compare_test() -> _.
simple_featurefull_schema_compare_test() ->
    ?assertEqual(
        {false, #{
            1 => #{
                0 => ?difference,
                1 => #{2 => ?difference},
                2 => #{3 => #{31 => #{0 => ?difference}}},
                3 => #{4 => #{41 => #{412 => ?difference}}}
            }
        }},
        begin
            Features = read(?SCHEMA, ?REQUEST),
            OtherFeatures = read(?SCHEMA, ?OTHER_REQUEST),

            compare(Features, OtherFeatures)
        end
    ).

-spec simple_featurefull_schema_list_diff_fields_test() -> _.
simple_featurefull_schema_list_diff_fields_test() ->
    ?assertEqual(
        [<<"1.0">>, <<"1.1.2">>, <<"1.2.31.0">>, <<"1.3.41.412">>],
        begin
            Features = read(?SCHEMA, ?REQUEST),
            OtherFeatures = read(?SCHEMA, ?OTHER_REQUEST),

            {false, Diff} = compare(Features, OtherFeatures),
            list_diff_fields(?SCHEMA, Diff)
        end
    ).

-endif.
