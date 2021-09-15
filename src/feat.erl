-module(feat).

-include("feat.hrl").

-type request_key() :: binary().
-type request_value() :: integer() | float() | binary() | request() | [request()] | undefined.
-type request() :: #{request_key() := request_value()}.

-type accessor() :: request_key() | nonempty_list(request_key()).

-type simple_schema() :: #{feature_name() := accessor() | {accessor(), schema()} | inner_schema()}.
-type seq_schema() :: set_schema().
-type set_schema() :: {set, inner_schema()}.
-type union_variants() :: #{request_value() => {feature_name(), inner_schema()}}.
-type union_schema() :: {union, accessor(), union_variants()}.

-type inner_schema() ::
    simple_schema() | union_schema().

-type schema() ::
    simple_schema()
    | seq_schema()
    | union_schema().

-type feature_name() :: non_neg_integer().
-type field_feature() :: integer() | undefined.
-type feature_value() :: field_feature() | features().
-type simple_features() :: #{feature_name() := feature_value()}.
-type seq_features() :: set_features().
-type set_features() :: [{Index :: integer(), feature_value()}].
-type union_features() :: {feature_name(), simple_features()}.
-type features() :: simple_features() | seq_features() | union_features().

-type total_difference() :: ?difference.
-type simple_difference() :: #{feature_name() := difference()}.
-type union_difference() :: {feature_name(), difference()}.
-type difference() ::
    total_difference()
    | simple_difference()
    | union_difference().

-type event() ::
    {missing_union_variant, Variant :: request_value(), request(), union_schema()}
    | {invalid_schema_fragment, feature_name(), request()}
    | {request_visited, {request, request()}}
    | {request_key_index_visit, integer()}
    | {request_key_index_visited, integer()}
    | {request_key_visit, {key, integer(), request()}}
    | {request_key_visited, {key, integer()}}.

-type no_return(_T) :: no_return().

-type error() ::
    {invalid_schema, term()}
    | {invalid_union_variant_schema, Variant :: request_value(), Data :: term(), union_schema()}.

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
-export([list_diff_fields/2]).
-export([hash/1]).

-callback handle_event(event(), options()) -> ok.

%% TODO: Read idea: why not ignore undefined fields all-together? better storage capacity

-spec read(schema(), request()) -> features() | no_return(error()).
read(Schema, Request) ->
    read(get_event_handler(), Schema, Request).

-spec read(event_handler(), schema(), request()) -> features().
read(Handler, Schema, Request) ->
    handle_event(Handler, {request_visited, {request, Request}}),
    read_(Schema, Request, Handler).

read_(_, undefined, _Handler) ->
    undefined;
read_(SeqSchema, RequestList, Handler) when is_list(RequestList) ->
    read_seq_(SeqSchema, RequestList, Handler);
read_(InnerSchema, Request, Handler) ->
    read_inner_(InnerSchema, Request, Handler).

read_seq_({set, Schema}, RequestList, Handler) ->
    {_, ListIndex} = lists:foldl(fun(Item, {N, Acc}) -> {N + 1, [{N, Item} | Acc]} end, {0, []}, RequestList),

    ListSorted = lists:keysort(2, ListIndex),
    lists:foldl(
        fun({Index, Req}, Acc) ->
            handle_event(Handler, {request_key_index_visit, Index}),
            Value = read_(Schema, Req, Handler),
            handle_event(Handler, {request_key_index_visited, Index}),
            [{Index, Value} | Acc]
        end,
        [],
        ListSorted
    ).

read_inner_({union, Accessor, Variants} = UnionSchema, Request, Handler) ->
    VariantName = read_raw_request_value(Accessor, Request, Handler),
    case maps:find(VariantName, Variants) of
        error ->
            handle_event(Handler, {missing_union_variant, VariantName, Request, UnionSchema}),
            undefined;
        {ok, {Feature, InnerSchema}} when is_integer(Feature) ->
            {Feature, read_inner_(InnerSchema, Request, Handler)};
        {ok, Data} ->
            error({invalid_union_variant_schema, VariantName, Data, UnionSchema})
    end;
read_inner_(Schema, Request, Handler) ->
    read_simple_(Schema, Request, Handler).

read_simple_(Schema, Request, Handler) when is_map(Schema) ->
    maps:fold(
        fun
            (_Name, 'reserved', Acc) ->
                Acc;
            (Name, NestedSchema, Acc) ->
                Acc#{Name => read_(NestedSchema, Request, Handler)}
        end,
        #{},
        Schema
    );
read_simple_({Accessor, NestedSchema}, Request, Handler) ->
    NestedRequest = read_raw_request_value(Accessor, Request, Handler),
    read_(NestedSchema, NestedRequest, Handler);
read_simple_(Accessor, Request, Handler) when is_binary(Accessor) ->
    read_hashed_request_value(Accessor, Request, Handler);
%% Finally falling from `read` to here: schema is invalid
read_simple_(Schema, _Request, _Handler) ->
    error({invalid_schema, Schema}).

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

read_request_value(Accessor, Value, Handler) ->
    read_request_value_(accessor_to_path(Accessor), Value, Handler).

read_request_value_(_, undefined, _) ->
    undefined;
read_request_value_([], Value, _) ->
    {ok, Value};
read_request_value_([Key | Rest], Request, Handler) when is_binary(Key), is_map(Request) ->
    case maps:find(Key, Request) of
        {ok, SubRequest} ->
            handle_event(Handler, {request_key_visit, {key, Key, SubRequest}}),
            Result = read_request_value_(Rest, SubRequest, Handler),
            handle_event(Handler, {request_key_visited, {key, Key}}),
            Result;
        error ->
            undefined
    end;
read_request_value_(Key, Request, Handler) ->
    handle_event(Handler, {invalid_schema_fragment, Key, Request}).

handle_event(undefined, {missing_union_variant, VariantName, Request, Schema}) ->
    logger:warning("Missing union variant ~p in request subset: ~p for schema  ~p", [VariantName, Request, Schema]),
    undefined;
handle_event(undefined, {invalid_schema_fragment, Key, Request}) ->
    logger:warning("Unable to extract idemp feature with schema: ~p from client request subset: ~p", [Key, Request]),
    undefined;
handle_event(undefined, _Event) ->
    ok;
handle_event({Mod, Opts}, Event) ->
    Mod:handle_event(Event, Opts).

get_event_handler() ->
    genlib_app:env(feat, event_handler, undefined).

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

compare_features(Fs, FsWith) when is_map(Fs), is_map(FsWith) ->
    compare_simple_features(Fs, FsWith);
compare_features(Fs, FsWith) when is_list(Fs), is_list(FsWith) ->
    compare_list_features(Fs, FsWith);
compare_features(Fs, FsWith) when tuple_size(Fs) == 2, tuple_size(FsWith) == 2 ->
    compare_union_features(Fs, FsWith);
%% We expect that clients may _at any time_ change their implementation and start
%% sending information they were not sending beforehand, so this is not considered a
%% conflict.
%% Yet, we DO NOT expect them to do the opposite, to stop sending
%% information they were sending, this is still a conflict.
compare_features(_, undefined) ->
    #{};
compare_features(Fs, Fs) ->
    #{};
%% Finally, values do not match at all
compare_features(_, _) ->
    ?difference.

%% Simple
compare_simple_features(Fs, FsWith) when is_map(Fs), is_map(FsWith) ->
    acc_to_diff(
        feat_utils:zipfold(
            fun(Key, Value, ValueWith, Acc) ->
                Diff = compare_features(Value, ValueWith),
                accumulate(Key, Diff, Acc)
            end,
            init_acc(),
            Fs,
            FsWith
        )
    ).

compare_list_features(L1, L2) when length(L1) =/= length(L2) ->
    ?difference;
compare_list_features(L1, L2) ->
    compare_list_features_(L1, L2, init_acc()).

compare_list_features_([], [], Acc) ->
    acc_to_diff(Acc);
compare_list_features_([{Index, V1} | Values], [{_, V2} | ValuesWith], Acc) ->
    Diff = compare_features(V1, V2),
    compare_list_features_(Values, ValuesWith, accumulate(Index, Diff, Acc)).

compare_union_features({Variant, _}, {VariantWith, _}) when Variant /= VariantWith ->
    ?difference;
compare_union_features({VariantFeature, Features}, {_, FeaturesWith}) when is_map(Features), is_map(FeaturesWith) ->
    case compare_simple_features(Features, FeaturesWith) of
        %% forwarding no-change for correct minimization
        M when map_size(M) == 0 ->
            #{};
        Diff ->
            {VariantFeature, Diff}
    end.

%% Acc values
%% 1. Usually {ActualDiffAcc, SimpleDiffCount} (simple diff = ?difference and not nested map)
%% 2. ?difference if it's union schema with changed discriminator (to utilize optimization)
init_acc() ->
    {#{}, 0}.

accumulate(Key, ?difference, {DiffAcc, SimpleCount}) ->
    {DiffAcc#{Key => ?difference}, SimpleCount + 1};
%% At least one value is the same: should show it in the result with level of detalization
%% By decrementing SimpleCount we ensure that acc_to_diff works correctly for this case (see below) by making
%% map_size(DiffAcc) and SimpleCount effectively diverge
accumulate(_, EmptyDiff, {DiffAcc, SimpleCount}) when map_size(EmptyDiff) == 0 ->
    {DiffAcc, SimpleCount - 1};
accumulate(Key, Diff, {DiffAcc, SimpleCount}) ->
    {DiffAcc#{Key => Diff}, SimpleCount}.

acc_to_diff(?difference) ->
    ?difference;
%% No nested diffs were added: technically, data is the same. Possible cases:
%% 1. Nested schema is empty (w/o features)
%% 2. It's a set schema with empty data in both requests
acc_to_diff({EmptyDiff, 0}) when map_size(EmptyDiff) == 0 ->
    #{};
acc_to_diff({SimpleDiff, SimpleCount}) when map_size(SimpleDiff) == SimpleCount ->
    ?difference;
%% Cases:
%% 1. Contains at least 1 complex diff
%% 2. At least 1 field is the same between two featuresets
acc_to_diff({Diff, _}) ->
    Diff.

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

list_diff_fields_(?difference, Schema, {PathsAcc, PathRev}) ->
    Path = lists:reverse(PathRev) ++ get_path(Schema),
    {[Path | PathsAcc], PathRev};
list_diff_fields_(Diffs, {set, Schema}, Acc) ->
    maps:fold(
        fun(I, Diff, {PathsAcc, PathRev}) ->
            {NewPathsAcc, _NewPathRev} = list_diff_fields_(Diff, Schema, {PathsAcc, [I | PathRev]}),
            {NewPathsAcc, PathRev}
        end,
        Acc,
        Diffs
    );
list_diff_fields_(Diff, {Accessor, Schema}, {PathsAcc, PathRev}) ->
    Path = accessor_to_path(Accessor),
    list_diff_fields_(Diff, Schema, {PathsAcc, lists:reverse(Path) ++ PathRev});
list_diff_fields_(Diff, Schema, Acc) ->
    list_diff_fields_inner_(Diff, Schema, Acc).

%% Simple
list_diff_fields_inner_(Diff, Schema, Acc) when is_map(Schema) ->
    list_diff_fields_simple_(Diff, Schema, Acc);
%% Union
%% If discriminator is different, diff would've been minimized because of it:
%% => implying, discriminator is not different here
list_diff_fields_inner_({_, ?difference}, UnionSchema, {PathsAcc, PathRev}) when element(1, UnionSchema) == union ->
    Path = lists:reverse(PathRev),
    {[Path | PathsAcc], PathRev};
list_diff_fields_inner_({Variant, Diff}, {union, _Accessor, Variants}, {PathsAcc, PathRev}) ->
    {DisValue, {_, VariantSchema}} =
        genlib_map:search(
            fun(_DisValue, {FeatureName, _Schema}) -> FeatureName =:= Variant end,
            Variants
        ),

    list_diff_fields_inner_(Diff, VariantSchema, {PathsAcc, [DisValue | PathRev]}).

list_diff_fields_simple_(Diff, Schema, Acc) when is_map(Diff), is_map(Schema) ->
    feat_utils:zipfold(
        fun(_Feature, DiffPart, SchemaPart, {_PathsAcc, PathRev} = AccIn) ->
            {NewPathsAcc, _NewPathRev} = list_diff_fields_(DiffPart, SchemaPart, AccIn),
            {NewPathsAcc, PathRev}
        end,
        Acc,
        Diff,
        Schema
    ).

get_path({Accessor, _Schema}) ->
    accessor_to_path(Accessor);
get_path(Accessor) when is_list(Accessor); is_binary(Accessor) ->
    accessor_to_path(Accessor);
get_path(_) ->
    [].

accessor_to_path(Key) when is_binary(Key) ->
    [Key];
accessor_to_path(Keys) when is_list(Keys) ->
    Keys.

-spec hash(term()) -> integer().
hash(V) ->
    erlang:phash2(V).
