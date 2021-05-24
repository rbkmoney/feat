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
        ?discriminator := request_key(),
        feature_name() := schema()
    }.
-type difference() :: ?difference | #{request_key() := difference()}.

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
        read_request_value(
            wrap_accessor(DiscriminatorAccessor),
            Request,
            Handler
        ),

    maps:fold(
        fun(Name, Schema, Acc) ->
            VariantValue = read_(Schema, Request, Handler),
            Acc#{Name => VariantValue}
        end,
        #{?discriminator => hash(DiscriminatorValue)},
        maps:remove(?discriminator, UnionSchema)
    );
read_(Schema, Request, Handler) when is_map(Schema) ->
    Result = maps:fold(
        fun
            (_Name, 'reserved', Acc) ->
                Acc;
            (Name, {Accessor, NestedSchema}, Acc) ->
                AccessorList = wrap_accessor(Accessor),
                NestedRequest = read_request_value(AccessorList, Request, Handler),
                Value = read_(NestedSchema, NestedRequest, Handler),
                Acc#{Name => Value};
            (Name, Accessor, Acc) ->
                AccessorList = wrap_accessor(Accessor),
                FeatureValue = read_request_value(AccessorList, Request, Handler),
                Acc#{Name => hash(FeatureValue)}
        end,
        #{},
        Schema
    ),
    Result.

read_request_value([], undefined, _) ->
    undefined;
read_request_value([], Value, _) ->
    Value;
read_request_value([Key | Rest], Request = #{}, Handler) when is_binary(Key) ->
    SubRequest = maps:get(Key, Request, undefined),
    handle_event(get_event_handler(Handler), {request_key_visit, {key, Key, SubRequest}}),
    Value = read_request_value(Rest, SubRequest, Handler),
    handle_event(get_event_handler(Handler), {request_key_visited, {key, Key}}),
    Value;
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
    Path = read_accessor(Accessor),
    list_diff_fields_(Diff, Schema, {PathsAcc, lists:reverse(Path) ++ [PathRev]});
list_diff_fields_(?difference, Accessor, {PathsAcc, PathRev}) ->
    Path = read_accessor(Accessor),
    FullPath = lists:reverse(PathRev) ++ Path,
    {[FullPath | PathsAcc], PathRev}.

wrap_accessor(Accessor) ->
    if
        is_list(Accessor) ->
            Accessor;
        is_binary(Accessor) ->
            [Accessor]
    end.

get_path({Accessor, _Schema}) ->
    read_accessor(Accessor);
get_path(Accessor) ->
    read_accessor(Accessor).

read_accessor(Key) when is_binary(Key) ->
    [Key];
read_accessor(Keys) when is_list(Keys) ->
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
    minimize_diff(compare_features_(Fs, FsWith)).

compare_features_(Fs, FsWith) ->
    zipfold(
        fun
            (Key, Values, ValuesWith, Diff) when is_list(ValuesWith), is_list(Values) ->
                compare_list_features(Key, Values, ValuesWith, Diff);
            (Key, Value, ValueWith, Diff) when is_map(ValueWith) and is_map(Value) ->
                compare_nested_features_(Key, Value, ValueWith, Diff);
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
    ).

compare_list_features(Key, L1, L2, Diff) when length(L1) =/= length(L2) ->
    Diff#{Key => ?difference};
compare_list_features(Key, L1, L2, Acc) ->
    Diff = compare_list_features_(L1, L2, #{}),
    case Diff of
        _ when map_size(Diff) > 0 ->
            Acc#{Key => minimize_diff(Diff)};
        #{} ->
            Acc
    end.

compare_list_features_([], [], Diff) ->
    Diff;
compare_list_features_([[Index, V1] | Values], [[_, V2] | ValuesWith], Acc) ->
    Diff = compare_nested_features_(Index, V1, V2, Acc),
    compare_list_features_(Values, ValuesWith, Diff).

compare_nested_features_(Key, Value, ValueWith, Diff) when is_map(Value) and is_map(ValueWith) ->
    case compare_features_(Value, ValueWith) of
        #{?discriminator := _} ->
            % Different with regard to discriminator, semantically same as different everywhere.
            Diff#{Key => ?difference};
        % different everywhere
        NestedDiff when map_size(NestedDiff) > 0 ->
            Diff#{Key => minimize_diff(NestedDiff)};
        #{} ->
            % no notable differences
            Diff
    end.

minimize_diff(?difference) ->
    ?difference;
minimize_diff(EmptyDiff) when map_size(EmptyDiff) == 0 ->
    #{};
minimize_diff(Diff) ->
    case complex_diff_count(Diff) of
        0 ->
            ?difference;
        _ ->
            Diff
    end.

complex_diff_count(Diff) ->
    maps:fold(
        fun
            (?discriminator, _, Count) -> Count;
            (_, ?difference, Count) -> Count;
            (_, _, Count) -> Count + 1
        end,
        0,
        Diff
    ).

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
