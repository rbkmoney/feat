-module(feat).

-include("feat.hrl").

-type request_key() :: binary().
-type request_value() :: integer() | float() | binary() | request() | [request()] | undefined.
-type request() :: #{request_key() := request_value()}.

-type accessor() :: request_key() | nonempty_list(request_key()).

-type map_schema() :: #{feature_name() := accessor() | {accessor(), schema()} | inner_schema()}.
-type seq_schema() :: set_schema().
-type set_schema() :: {set, inner_schema()}.
-type union_variants() :: #{request_value() := {feature_name(), inner_schema()}}.
-type union_schema() :: {union, accessor(), union_variants()}.

-type inner_schema() ::
    map_schema() | union_schema().

-type schema() ::
    map_schema()
    | seq_schema()
    | union_schema().

%% Special type that denotes a list with strict number and order of elements,
%% just as tuples do, but is impossible to describe with Erlang typespecs
%%
%% The reason for storing values as lists opposing to tuples is for better serialization:
%% although the tuple is a useful primitive, most formats support only arrays/lists
%%
%% So the goal here is to achieve effortless `cool_format:encode(Features)`
%% as long as the format provides array and map primitives
-type list_tuple(A, B) :: [A | B].

-type feature_name() :: non_neg_integer().
-type field_feature() :: integer() | undefined.
-type seq_index() :: integer().
-type feature_value() :: field_feature() | features().
-type map_features() :: #{feature_name() := feature_value()}.
-type seq_features() :: set_features().
-type set_features() :: [list_tuple(Index :: integer(), feature_value())].
-type union_features() :: list_tuple(feature_name(), map_features()).
-type features() :: map_features() | seq_features() | union_features().

-type total_difference() :: ?difference.
-type map_difference() :: #{feature_name() := difference()}.
-type union_difference() :: {feature_name(), difference()}.
-type difference() ::
    total_difference()
    | map_difference()
    | union_difference().

%% MAYBE: Currently events are somewhat restricted in what you can do with them,
%% because you don't have a complete context, e.g path (although you can build one from scratch)
%% Have to evaluate the applications of events in prod
-type event() ::
    %% Entry
    {request_visited, request()}
    %% Nested
    | {request_key_visit, request_key(), Value :: request()}
    | {request_key_visited, request_key(), Value :: request()}
    %% Seq
    | {request_index_visit, seq_index(), Value :: request()}
    | {request_index_visited, seq_index(), Value :: request()}
    %% Unions
    | {request_variant_visit, feature_name(), Variant :: request_value(), Value :: request()}
    | {request_variant_visited, feature_name(), Variant :: request_value(), Value :: request()}
    | {missing_union_variant, Variant :: request_value(), request(), union_schema()}
    %% Data or Schema errors (depends on Correct data representation)
    | {invalid_schema_fragment, request_key(), request()}
    | {missing_union_variant_value, request(), union_schema()}.

-type no_return(_T) :: no_return().

-type error() ::
    {invalid_schema, term()}
    | {invalid_union_variants, union_schema()}
    | {invalid_union_variant_schema, Variant :: request_value(), InvalidVariantSchema :: term(), union_schema()}.

-type event_handler() :: {module(), options()} | fun((event()) -> ok) | undefined.
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
read(Handler, Schema, Request) when tuple_size(Handler) =:= 2; is_function(Handler, 1); Handler =:= undefined ->
    handle_event(Handler, {request_visited, Request}),
    do_read(Schema, Request, Handler).

do_read(_, undefined, _Handler) ->
    undefined;
do_read(MapSchema = #{}, Request, Handler) ->
    do_read_map(MapSchema, Request, Handler);
do_read({union, Accessor, Variants}, Request, Handler) ->
    do_read_union(Accessor, Variants, Request, Handler);
do_read({set, Schema}, RequestList, Handler) when is_list(RequestList) ->
    do_read_set(Schema, RequestList, Handler);
do_read({Accessor, Schema}, Request, Handler) ->
    do_read_nested(Accessor, Schema, Request, Handler);
do_read(Accessor, Request, Handler) when is_binary(Accessor); is_list(Accessor) ->
    do_read_accessor(Accessor, Request, Handler);
do_read(InvalidSchema, _Request, _Handler) ->
    error({invalid_schema, InvalidSchema}).

do_read_set(Schema, RequestList, Handler) ->
    {_, ListIndex} = lists:foldl(fun(Item, {N, Acc}) -> {N + 1, [{N, Item} | Acc]} end, {0, []}, RequestList),

    ListSorted = lists:keysort(2, ListIndex),
    lists:foldl(
        fun({Index, Req}, Acc) ->
            handle_event(Handler, {request_index_visit, Index, Req}),
            Value = do_read(Schema, Req, Handler),
            handle_event(Handler, {request_index_visited, Index, Req}),
            [[Index, Value] | Acc]
        end,
        [],
        ListSorted
    ).

do_read_union(TypeAccessor, Variants, Request, Handler) ->
    VariantResult =
        read_request_value(
            TypeAccessor,
            Request,
            Handler,
            fun
                ({ok, Variant}) ->
                    do_read_union_variant(Variant, TypeAccessor, Variants, Request, Handler);
                (error) ->
                    handle_event(Handler, {missing_union_variant_value, Request, {union, TypeAccessor, Variants}}),
                    undefined
            end
        ),

    case VariantResult of
        undefined ->
            undefined;
        {ok, Variant, Feature, InnerSchema} ->
            handle_event(Handler, {request_variant_visit, Feature, Variant, Request}),
            Result = [Feature, do_read_map(InnerSchema, Request, Handler)],
            handle_event(Handler, {request_variant_visited, Feature, Variant, Request}),
            Result
    end.

do_read_union_variant(Variant, TypeAccessor, Variants = #{}, Request, Handler) ->
    case maps:find(Variant, Variants) of
        {ok, {Feature, InnerSchema}} when is_integer(Feature) ->
            {ok, Variant, Feature, InnerSchema};
        {ok, InvalidVariantSchema} ->
            error({invalid_union_variant_schema, Variant, InvalidVariantSchema, {union, TypeAccessor, Variants}});
        error ->
            handle_event(Handler, {missing_union_variant, Variant, Request, {union, TypeAccessor, Variants}}),
            undefined
    end;
do_read_union_variant(_Variant, TypeAccessor, NotVariants, _Request, _Handler) ->
    error({invalid_union_variants, {union, TypeAccessor, NotVariants}}).

do_read_map(Schema, Request, Handler) when is_map(Schema) ->
    maps:fold(
        fun
            (_Name, 'reserved', Acc) ->
                Acc;
            (Name, NestedSchema, Acc) ->
                Acc#{Name => do_read(NestedSchema, Request, Handler)}
        end,
        #{},
        Schema
    ).

do_read_nested(Accessor, Schema, Request, Handler) ->
    read_request_value(
        Accessor,
        Request,
        Handler,
        fun(Arg) ->
            NestedRequest =
                case Arg of
                    {ok, Value} -> Value;
                    error -> undefined
                end,
            do_read(Schema, NestedRequest, Handler)
        end
    ).

do_read_accessor(Accessor, Request, Handler) ->
    read_request_value(
        Accessor,
        Request,
        Handler,
        fun
            ({ok, Value}) -> hash(Value);
            (error) -> undefined
        end
    ).

read_request_value(Accessor, Value, Handler, Then) when is_function(Then, 1) ->
    read_request_value_(accessor_to_path(Accessor), Value, Handler, Then).

read_request_value_(_, undefined, _, Then) ->
    Then(error);
read_request_value_([], Value, _, Then) ->
    Then({ok, Value});
read_request_value_([Key | Rest], Request, Handler, Then) when is_binary(Key), is_map(Request) ->
    case maps:find(Key, Request) of
        {ok, SubRequest} ->
            handle_event(Handler, {request_key_visit, Key, SubRequest}),
            Result = read_request_value_(Rest, SubRequest, Handler, Then),
            handle_event(Handler, {request_key_visited, Key, SubRequest}),
            Result;
        error ->
            Then(error)
    end;
read_request_value_(Key, Request, Handler, _Then) ->
    handle_event(Handler, {invalid_schema_fragment, Key, Request}),
    undefined.

%% DISCUSS: do we need this "ok" though?
handle_event(Handler, Event) ->
    ok =
        case Handler of
            FunHandler when is_function(FunHandler, 1) ->
                FunHandler(Event);
            {Mod, Opts} ->
                Mod:handle_event(Event, Opts);
            undefined ->
                default_handle_event(Event)
        end.

default_handle_event({missing_union_variant, Variant, Request, Schema}) ->
    logger:warning("Missing union variant ~p in request subset: ~p for schema  ~p", [Variant, Request, Schema]),
    ok;
default_handle_event({invalid_schema_fragment, Key, Request}) ->
    logger:warning("Unable to extract idemp feature with schema: ~p from client request subset: ~p", [Key, Request]),
    ok;
default_handle_event(_) ->
    ok.

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
    compare_map_features(Fs, FsWith);
%% NOTE: this seems very fragile, but if the contract for sets (list of lists) stands, this will do
compare_features([VIndex1, _] = Fs, [VIndex2, _] = FsWith) when is_integer(VIndex1), is_integer(VIndex2) ->
    compare_union_features(Fs, FsWith);
compare_features(Fs, FsWith) when is_list(Fs), is_list(FsWith) ->
    compare_list_features(Fs, FsWith);
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

compare_map_features(Fs, FsWith) when is_map(Fs), is_map(FsWith) ->
    acc_to_diff(
        genlib_map:zipfold(
            fun(Key, Value, ValueWith, Acc) ->
                Diff = compare_features(Value, ValueWith),
                accumulate(Key, Diff, Acc)
            end,
            init_acc(),
            Fs,
            FsWith
        )
    ).

compare_union_features([Variant, _], [VariantWith, _]) when Variant /= VariantWith ->
    ?difference;
compare_union_features([VariantFeature, Features], [_, FeaturesWith]) when is_map(Features), is_map(FeaturesWith) ->
    case compare_map_features(Features, FeaturesWith) of
        %% forwarding no-change for correct minimization
        M when map_size(M) == 0 ->
            #{};
        Diff ->
            [VariantFeature, Diff]
    end.

compare_list_features(L1, L2) when length(L1) =/= length(L2) ->
    ?difference;
compare_list_features(L1, L2) ->
    compare_list_features_(L1, L2, init_acc()).

compare_list_features_([], [], Acc) ->
    acc_to_diff(Acc);
compare_list_features_([[Index, V1] | Values], [[_, V2] | ValuesWith], Acc) ->
    Diff = compare_features(V1, V2),
    compare_list_features_(Values, ValuesWith, accumulate(Index, Diff, Acc)).

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
    lists:sort(
        lists:map(
            fun(Keys) -> list_to_binary(lists:join(<<".">>, Keys)) end,
            unroll_pathmap(build_pathmap(Diff, Schema))
        )
    ).

unroll_pathmap(Pathmap) ->
    maps:fold(
        fun
            (Key, Empty, Acc) when map_size(Empty) =:= 0 ->
                [[Key] | Acc];
            (Key, Rest, Acc) ->
                lists:map(
                    fun(NestedPath) -> [Key | NestedPath] end,
                    unroll_pathmap(Rest)
                ) ++ Acc
        end,
        [],
        Pathmap
    ).

build_pathmap(?difference, Accessor) when is_binary(Accessor); is_list(Accessor) ->
    nested_map(accessor_to_path(Accessor), #{});
build_pathmap(?difference, _Schema) ->
    #{};
build_pathmap(Diffs, {set, Schema}) ->
    build_pathmap_set(Diffs, Schema);
build_pathmap(Diff, {Accessor, Schema}) ->
    build_pathmap_nested(Diff, Accessor, Schema);
build_pathmap(Diff, Schema) when is_map(Schema) ->
    build_pathmap_map(Diff, Schema);
build_pathmap(Diff, {union, _Accessor, Variants}) ->
    build_pathmap_union(Diff, Variants).

build_pathmap_set(Diffs, Schema) ->
    maps:fold(
        fun(I, Diff, Acc) ->
            Acc#{integer_to_binary(I) => build_pathmap(Diff, Schema)}
        end,
        #{},
        Diffs
    ).

build_pathmap_nested(Diff, Accessor, Schema) ->
    nested_map(accessor_to_path(Accessor), build_pathmap(Diff, Schema)).

%% If discriminator is different, diff would've been minimized because of it:
%% => implying, discriminator is not different here
build_pathmap_union([_, ?difference], _Variants) ->
    #{};
build_pathmap_union([Variant, Diff], Variants) ->
    {DisValue, {_, VariantSchema}} =
        genlib_map:search(
            fun(_DisValue, {FeatureName, _Schema}) -> FeatureName =:= Variant end,
            Variants
        ),

    #{DisValue => build_pathmap_map(Diff, VariantSchema)}.

build_pathmap_map(Diff, Schema) ->
    genlib_map:zipfold(
        fun(_Feature, DiffPart, SchemaPart, Acc) ->
            merge_pathmaps(Acc, build_pathmap(DiffPart, SchemaPart))
        end,
        #{},
        Diff,
        Schema
    ).

merge_pathmaps(_Left, Right) when map_size(Right) =:= 0 ->
    #{};
merge_pathmaps(Left, Right) when map_size(Left) =:= 0 ->
    Right;
merge_pathmaps(Left, Right) ->
    maps:fold(
        fun(Key, RightValue, Acc) ->
            case maps:find(Key, Acc) of
                {ok, LeftValue} when
                    map_size(LeftValue) == 0;
                    map_size(RightValue) == 0
                ->
                    Acc#{Key => #{}};
                {ok, LeftValue} ->
                    Acc#{Key => merge_pathmaps(LeftValue, RightValue)};
                error ->
                    Acc#{Key => RightValue}
            end
        end,
        Left,
        Right
    ).

nested_map(Keys, InitAcc) ->
    lists:foldr(fun(Key, Acc) -> #{Key => Acc} end, InitAcc, Keys).

accessor_to_path(Key) when is_binary(Key) ->
    [Key];
accessor_to_path(Keys) when is_list(Keys) ->
    Keys.

-spec hash(term()) -> integer().
hash(V) ->
    erlang:phash2(V).
