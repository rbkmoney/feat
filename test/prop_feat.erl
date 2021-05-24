-module(prop_feat).

-include("feat.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(feat_utils, [traverse_schema/3]).

-define(PROP(Prop), ?PROP(Prop, [])).
-define(PROP(Prop, Opts), ?assert(proper:quickcheck(Prop, [{to_file, user}] ++ Opts))).

-define(NON_EMPTY(Spec), ?SUCHTHAT(X, Spec, X /= [])).
-define(SIZE_NON_EMPTY(Spec), ?SIZED(Size, resize(max(1, Size), Spec))).
-define(assertEqualSets(List1, List2), begin
    ?assertEqual(List1 -- List2, List2 -- List1),
    true
end).

-spec prop_hash_calculatable() -> proper:test().
prop_hash_calculatable() ->
    ?FORALL(Term, term(), is_integer(feat:hash(Term))).

%% TODO:
%% 1. prop_compare_scrambled_sets
%% 2. prop_compare_different_sets (change, delete, add)
%% 3. Invalid schema (e.g. #{0 => [<<"0">>],1 => [<<"0">>]})
%% 4. EUnit-embed for schema validation: ?FEAT_CHECK_SCHEMAS(SchemaOrSchemas) and ?assertValidFeatureSchema(Schema)
%% 5. Validation check: check union variant nested conflicts (e.g. different value kinds under same keys)?
-spec prop_read() -> proper:test().
prop_read() ->
    ?FORALL(
        Schema,
        schema(),
        ?FORALL(
            Entity,
            entity(Schema),
            begin
                Features = feat:read(Schema, Entity),

                is_map(Features) andalso
                    assert_correct_read(Schema, Features, Entity)
            end
        )
    ).

-spec prop_compare_same() -> proper:test().
prop_compare_same() ->
    ?FORALL(
        Schema,
        schema(),
        begin
            ?FORALL(
                [Entity1, PathSpecs],
                [fill_schema(Schema), random_nonexistent_pathspecs(Schema)],
                ?FORALL(
                    Entity2,
                    change_values_by_paths(PathSpecs, Entity1),
                    begin
                        Features1 = feat:read(Schema, Entity1),
                        Features2 = feat:read(Schema, Entity2),

                        ?assertEqual(true, feat:compare(Features1, Features2)),
                        true
                    end
                )
            )
        end
    ).

-spec prop_compare_different() -> proper:test().
prop_compare_different() ->
    ?FORALL(
        Schema,
        non_empty_schema(),
        ?FORALL(
            [Entity1, PathSpecs],
            [fill_schema(Schema), ?SIZE_NON_EMPTY(random_pathspecs_for_change(Schema))],
            ?FORALL(
                Entity2,
                change_values_by_paths(PathSpecs, Entity1),
                begin
                    Features1 = feat:read(Schema, Entity1),
                    Features2 = feat:read(Schema, Entity2),

                    {false, Diff} = feat:compare(Features1, Features2),

                    assert_correct_compare(Diff, PathSpecs)
                end
            )
        )
    ).

-spec prop_list_diff_fields() -> proper:test().
prop_list_diff_fields() ->
    ?FORALL(
        Schema,
        schema(),
        ?FORALL(
            [Entity1, PathSpecs],
            [fill_schema(Schema), ?SIZE_NON_EMPTY(random_pathspecs_for_change(Schema))],
            ?FORALL(
                Entity2,
                change_values_by_paths(PathSpecs, Entity1),
                begin
                    Features1 = feat:read(Schema, Entity1),
                    Features2 = feat:read(Schema, Entity2),

                    {false, Diff} = feat:compare(Features1, Features2),
                    DiffFields = feat:list_diff_fields(Schema, Diff),

                    case DiffFields of
                        all ->
                            %% TODO: more complex check
                            true;
                        DiffFields when is_list(DiffFields) ->
                            ChangedFields =
                                pathspecs_to_binpaths(PathSpecs, Diff),

                            is_map(Diff) andalso
                                ?assertEqualSets(ChangedFields, DiffFields)
                    end
                end
            )
        )
    ).

non_empty_schema() ->
    ?SIZE_NON_EMPTY(schema()).

schema() ->
    schema(#{}).

schema(Opts) ->
    ?SIZED(
        Size,
        ?LET(
            Features,
            features(Size),
            begin
                Schema = generate_schema(Opts, Features),
                IsEmptySchema = (map_size(Schema) == 0) or (schema_score(Schema) == 0),
                case IsEmptySchema of
                    true ->
                        schema(Opts);
                    false ->
                        Schema
                end
            end
        )
    ).

generate_schema(_, []) ->
    #{};
generate_schema(Opts, Features) ->
    FeatureCount = length(Features),
    DefaultOpts = #{
        avg_depth => rand1(FeatureCount),
        max_union_variants => rand1(FeatureCount),
        max_nested_elements_perc => 1.0
    },

    FinalOpts = maps:merge(DefaultOpts, Opts),
    do_generate_schema(FinalOpts, Features, #{}).

%% TODO: opts: empty_unions, etc.
do_generate_schema(_Opts, [], Acc) ->
    Acc;
do_generate_schema(Opts, CurrentFeatures = [{FeatureID, FeatureName} | RestFeatures], Acc) ->
    AvgDepth = maps:get(avg_depth, Opts, 3),

    DiceReserved = dice(10),
    DiceNested = not dice(AvgDepth + 1),
    DiceUnion = dice(2),

    {NextFeatures, NextAcc} =
        if
            DiceReserved ->
                {RestFeatures, maps:put(FeatureID, 'reserved', Acc)};
            %% Sets and Nested Schemas
            DiceNested, not DiceUnion, RestFeatures /= [] ->
                do_generate_set_or_nested(Opts, CurrentFeatures, Acc);
            %% DiscriminatedSchema aka Union
            DiceNested, DiceUnion, tl(RestFeatures) /= [] ->
                do_generate_union(Opts, CurrentFeatures, Acc);
            %% Simple Value
            true ->
                {RestFeatures, maps:put(FeatureID, FeatureName, Acc)}
        end,
    do_generate_schema(Opts, NextFeatures, NextAcc).

do_generate_set_or_nested(Opts, CurrentFeatures = [{FeatureID, FeatureName} | RestFeatures], Acc) ->
    {NestedFeatures, LeftFeatures} = lists_split_by_perc(
        randf(maps:get(max_nested_elements_perc, Opts)),
        RestFeatures
    ),
    NestedSchema = do_generate_schema(
        Opts,
        NestedFeatures,
        #{}
    ),

    case map_size(NestedSchema) of
        0 ->
            {CurrentFeatures, Acc};
        _ ->
            DiceIsSet = dice(2),
            RequestSchema =
                case DiceIsSet of
                    true -> {set, NestedSchema};
                    false -> NestedSchema
                end,
            {LeftFeatures, maps:put(FeatureID, {FeatureName, RequestSchema}, Acc)}
    end.

do_generate_union(Opts, CurrentFeatures = [{FeatureID, FeatureName} | RestFeatures], Acc) ->
    [{_, DiscriminatorName} | RestFeatures1] = RestFeatures,
    MaxUnionVariants = min(
        maps:get(max_union_variants, Opts, 3),
        length(RestFeatures1)
    ),
    VariantCount = rand(MaxUnionVariants),

    {VariantFeatures, RestFeatures2} = lists:split(VariantCount, RestFeatures1),

    {VariantSchemaFeatures, NextRestFeatures} =
        lists_split_by_perc(
            randf(maps:get(max_nested_elements_perc, Opts)),
            RestFeatures2
        ),

    %% TODO: Can't features be reused across union elements? Or it's schema error (check during tests)
    UnionSchema =
        lists:foldl(
            fun({{VariantFeatureID, _VariantFeatureName}, VariantNestedFeatures}, UnionAcc) ->
                VariantSchema =
                    do_generate_schema(
                        Opts,
                        VariantNestedFeatures,
                        #{}
                    ),

                case map_size(VariantSchema) of
                    0 -> UnionAcc;
                    _ -> maps:put(VariantFeatureID, VariantSchema, UnionAcc)
                end
            end,
            #{?discriminator => DiscriminatorName},
            lists:zip(
                VariantFeatures,
                lists_split_randomly(VariantCount, VariantSchemaFeatures)
            )
        ),

    %% If generation used features but produced almost empty union schema -- skip this iteration
    case map_size(UnionSchema) == 1 andalso VariantFeatures /= [] andalso VariantSchemaFeatures /= [] of
        true -> {CurrentFeatures, Acc};
        false -> {NextRestFeatures, maps:put(FeatureID, {FeatureName, UnionSchema}, Acc)}
    end.

features(Size) ->
    ?LET(
        Names,
        ?SUCHTHAT(
            Names,
            resize(Size, list(identifier())),
            length(Names) == length(sets:to_list(sets:from_list(Names)))
        ),
        lists:zip(lists:seq(1, length(Names)), Names)
    ).

identifier() ->
    ?LET(
        [Parts, Separator],
        %% TODO: Compare tests fail without SUCHTHAT... Reason?
        [
            ?SUCHTHAT(
                L,
                list(alphanum_char()),
                L /= []
            ),
            oneof([$_, $-])
        ],
        erlang:list_to_binary(lists:join(Separator, Parts))
    ).

alphanum_char() ->
    oneof([
        integer(16#30, 16#39),
        integer(16#41, 16#5A),
        integer(16#61, 16#7A)
    ]).

entity(Schema) ->
    fill_schema(Schema).

fill_schema(Schema) ->
    traverse_schema(
        fun
            (reserved, Acc, _) ->
                Acc;
            (value, Acc, RevPath) ->
                %% TODO: applicable term()s generation
                %% %% TODO?: sparsity (how many are not defined)
                Value = generate_unique_binary(),

                NamePath = name_path(lists:reverse(RevPath)),
                case deep_force_put(NamePath, Value, Acc) of
                    {ok, NewAcc} -> NewAcc;
                    {error, map_overwrite} -> Acc
                end;
            ({set, NestedSchema}, Acc, RevPath) ->
                Elements =
                    lists:map(
                        fun(_) -> fill_schema(NestedSchema) end,
                        lists:seq(1, rand:uniform(5))
                    ),

                NamePath = name_path(lists:reverse(RevPath)),
                NextAcc =
                    case deep_force_put(NamePath, Elements, Acc) of
                        {ok, NewAcc} -> NewAcc;
                        {error, map_overwrite} -> Acc
                    end,
                {no_traverse, NextAcc};
            ({union, DiscriminatorName, UnionSchema}, Acc, RevPath) ->
                UnionVariant =
                    case maps:to_list(UnionSchema) of
                        [] ->
                            #{};
                        UnionVariantsSchemas ->
                            {_Idx, UnionVariantSchema} = rand_elem(UnionVariantsSchemas),
                            fill_schema(UnionVariantSchema)
                    end,

                DiscriminatorValue = generate_unique_binary(),
                Value = maps:put(DiscriminatorName, DiscriminatorValue, UnionVariant),

                NamePath = name_path(lists:reverse(RevPath)),

                case deep_force_put(NamePath, Value, Acc) of
                    {ok, NewAcc} -> NewAcc;
                    {error, map_overwrite} -> Acc
                end;
            (_, Acc, _RevPath) ->
                Acc
        end,
        #{},
        Schema
    ).

assert_correct_read(Schema, Features, Entity) ->
    traverse_schema(
        fun
            (_, false, _) ->
                false;
            ('reserved', Acc, _) ->
                Acc;
            ({nested, _}, Acc, _) ->
                Acc;
            (Kind, true, RevPath) ->
                Path = lists:reverse(RevPath),
                FeatureResult = deep_fetch(id_path(Path), Features),
                EntityResult = deep_fetch(name_path(Path), Entity),
                Action =
                    case {FeatureResult, EntityResult} of
                        {{error, not_found, _}, {error, not_found, _}} ->
                            pass;
                        {{error, not_found, _}, {ok, _}} ->
                            throw({feature_unused, Path, Entity, Features, Schema});
                        {{ok, _}, {error, not_found, _}} ->
                            throw({unknown_feature, Path, Entity, Features, Schema});
                        {{ok, FoundFeatures}, {ok, FoundEntities}} when is_map(FoundFeatures) ->
                            {nested, FoundFeatures, FoundEntities};
                        {{ok, FeatureValue}, {ok, EntityValue}} ->
                            {ok, FeatureValue, EntityValue};
                        {{error, not_map, ErrorPath}, _} ->
                            throw({feature_not_map, ErrorPath, Path, Entity, Features, Schema});
                        {_, {error, not_map, ErrorPath}} ->
                            throw({entity_not_map, ErrorPath, Path, Entity, Features, Schema})
                    end,
                case {Kind, Action} of
                    {_, pass} ->
                        true;
                    {value, {ok, FeatureHash, Value}} ->
                        ?assertEqual(FeatureHash, feat:hash(Value)),
                        true;
                    {{set, _NestedSchema}, {ok, undefined, _NestedValues}} ->
                        %% Inside recursive union comparison: there's a field with the same noun
                        false;
                    {{set, NestedSchema}, {ok, NestedFeatureSet, NestedValues}} ->
                        NestedFeatures = lists:map(fun([_Index, Feats]) -> Feats end, NestedFeatureSet),
                        NestedEntities = lists:reverse(lists:sort(NestedValues)),
                        Result =
                            lists:foldl(
                                fun
                                    (_, false) ->
                                        false;
                                    ({FeatureElement, EntityElement}, true) ->
                                        assert_correct_read(NestedSchema, FeatureElement, EntityElement)
                                end,
                                true,
                                lists:zip(NestedFeatures, NestedEntities)
                            ),

                        {no_traverse, Result};
                    {{union, _DiscriminatorName, _UnionSchema}, {ok, undefined, _NestedValues}} ->
                        %% Inside recursive union comparison: there's a field with the same noun
                        false;
                    {{union, DiscriminatorName, UnionSchema}, {nested, NestedFeatures, NestedValues}} ->
                        assert_correct_read_union(DiscriminatorName, UnionSchema, NestedFeatures, NestedValues);
                    {_, {nested, _, _}} ->
                        throw({nested_features, Path})
                end
        end,
        true,
        Schema
    ).

assert_correct_read_union(DiscriminatorName, UnionSchema, NestedFeatures, NestedValues) ->
    DiscriminatorResult =
        maps:get(?discriminator, NestedFeatures) ==
            feat:hash(maps:get(DiscriminatorName, NestedValues)),

    OkUnionVariantsCount =
        maps:fold(
            fun(Idx, UnionVariantSchema, Acc) ->
                UnionVariantFeatures =
                    maps:get(Idx, NestedFeatures),

                try
                    assert_correct_read(
                        UnionVariantSchema,
                        UnionVariantFeatures,
                        NestedValues
                    )
                of
                    false -> Acc;
                    true -> Acc + 1
                catch
                    %% Union element miss
                    throw:UnknownFeature when element(1, UnknownFeature) == unknown_feature ->
                        Acc;
                    %% 2+ union elements with different structure and same nouns
                    throw:FeatureNotMap when element(1, FeatureNotMap) == feature_not_map ->
                        Acc
                end
            end,
            0,
            UnionSchema
        ),
    UnionVariantCount = map_size(UnionSchema),
    UnionVariantsResult = (OkUnionVariantsCount > 0) or (UnionVariantCount == 0),

    DiscriminatorResult and UnionVariantsResult.

%% TODO: use ?assert*
assert_correct_compare(Diff, Paths) ->
    do_assert_correct_compare(Diff, Paths, []).

%% TODO: check that all paths are different?
do_assert_correct_compare(?difference, _Paths, _RevPath) ->
    true;
do_assert_correct_compare(Diff, Paths, RevPath) ->
    lists:foldl(
        fun
            (_, false) ->
                false;
            ({value, Id, _Name}, _) ->
                case maps:find(Id, Diff) of
                    {ok, ?difference} ->
                        true;
                    _ ->
                        false
                end;
            ({nested, Id, Name, NestedPaths, NestedSchema}, _) ->
                NewRevPath = [{Id, Name} | RevPath],
                case maps:find(Id, Diff) of
                    {ok, NestedDiff} when is_map(NestedDiff) ->
                        do_assert_correct_compare(NestedDiff, NestedPaths, NewRevPath);
                    %% HACK: check that all paths are to change?
                    {ok, ?difference} ->
                        true;
                    error ->
                        logger:error("Nested diff is not found at ~p for subschema: ~p~n", [
                            lists:reverse(NewRevPath),
                            NestedSchema
                        ]),
                        false
                end;
            ({set, Id, Name, NestedPaths, NestedSchema}, _) ->
                assert_correct_compare_set(Id, Name, NestedPaths, NestedSchema, Diff, RevPath);
            ({union, Id, Name, DiscriminatorName, VariantPathSpecs, UnionSchema}, _) ->
                assert_correct_compare_union(Id, Name, DiscriminatorName, VariantPathSpecs, UnionSchema, Diff, RevPath)
        end,
        true,
        Paths
    ).

assert_correct_compare_set(Id, Name, NestedPaths, NestedSchema, Diff, RevPath) ->
    NewRevPath = [{Id, Name} | RevPath],
    case maps:find(Id, Diff) of
        %% TODO: how to check if length is indeed different in tests?
        {ok, ?difference} ->
            true;
        {ok, SetDiff} when is_map(SetDiff) ->
            maps:fold(
                fun
                    (_, _, false) ->
                        false;
                    (Idx, NestedDiff, _) ->
                        NextRevPath = [{Idx, Idx} | NewRevPath],
                        Result = do_assert_correct_compare(
                            NestedDiff,
                            NestedPaths,
                            NextRevPath
                        ),
                        case Result of
                            false ->
                                logger:error("Diff for ~p is incorrect", [NextRevPath]),
                                false;
                            true ->
                                true
                        end
                end,
                true,
                SetDiff
            );
        error ->
            logger:error("Set diff is not found at ~p for subschema: ~p~n", [
                lists:reverse(NewRevPath),
                NestedSchema
            ]),
            false
    end.
assert_correct_compare_union(Id, Name, DiscriminatorName, VariantPathSpecs, UnionSchema, Diff, RevPath) ->
    NewRevPath = [{Id, Name} | RevPath],
    case maps:find(Id, Diff) of
        {ok, ?difference} when DiscriminatorName /= undefined ->
            true;
        {ok, ?difference} ->
            logger:error(
                "Union diff at ~p shows total diff for unchanged discriminator: ~p~n",
                [
                    lists:reverse(NewRevPath),
                    UnionSchema
                ]
            ),
            false;
        {ok, NestedDiff} ->
            lists:foldl(
                fun
                    (_, false) ->
                        false;
                    ({Idx, PathSpec}, true) ->
                        case maps:find(Idx, NestedDiff) of
                            {ok, UnionVariantDiff} ->
                                do_assert_correct_compare(
                                    UnionVariantDiff,
                                    PathSpec,
                                    [{Idx, union} | NewRevPath]
                                );
                            error ->
                                logger:error(
                                    "Expected union element ~p at ~p is not found in diff ~p: ~p~n",
                                    [
                                        Idx,
                                        lists:reverse(NewRevPath),
                                        NestedDiff,
                                        UnionSchema
                                    ]
                                )
                        end
                end,
                true,
                VariantPathSpecs
            );
        error ->
            logger:error("Union diff is not found at ~p for subschema: ~p~n", [
                lists:reverse(NewRevPath),
                UnionSchema
            ]),
            false
    end.

pathspecs(Schema) ->
    traverse_schema(
        fun
            (reserved, Acc, _) ->
                Acc;
            (value, Acc, [{Id, Name} | _]) ->
                [{value, Id, Name} | Acc];
            ({nested, NestedSchema}, Acc, [{Id, Name} | _]) ->
                NestedPaths = pathspecs(NestedSchema),
                Element = {nested, Id, Name, NestedPaths, NestedSchema},
                NewAcc = [Element | Acc],
                {no_traverse, NewAcc};
            ({set, NestedSchema}, Acc, [{Id, Name} | _]) ->
                NestedPaths = pathspecs(NestedSchema),
                Element = {set, Id, Name, NestedPaths, NestedSchema},
                NewAcc = [Element | Acc],
                {no_traverse, NewAcc};
            ({union, DiscriminatorName, UnionSchema}, Acc, [{Id, Name} | _]) ->
                VariantPathSpecs =
                    maps:to_list(
                        maps:map(
                            fun(_VariantId, NestedSchema) -> pathspecs(NestedSchema) end,
                            UnionSchema
                        )
                    ),

                Element = {union, Id, Name, DiscriminatorName, VariantPathSpecs, UnionSchema},
                [Element | Acc]
        end,
        [],
        Schema
    ).

random_pathspecs_for_change(Schema) ->
    Score = schema_score(Schema),
    ?SIZED(
        Size,
        begin
            case Size == 0 orelse Score == 0 of
                true ->
                    [];
                false ->
                    ForceNonEmpty = true,
                    {false, PathSpecs} = do_random_pathspecs_for_change(
                        ForceNonEmpty,
                        min(Size, Score) / Score,
                        Schema
                    ),
                    PathSpecs
            end
        end
    ).

do_random_pathspecs_for_change(ForceNonEmptyInit, Prob, Schema) ->
    traverse_schema(
        fun
            (reserved, Acc, _) ->
                Acc;
            (value, {ForceNonEmpty, Acc}, [{Id, Name} | _]) ->
                case ForceNonEmpty orelse dice(Prob) of
                    true ->
                        {false, [{value, Id, Name} | Acc]};
                    false ->
                        {false, Acc}
                end;
            ({nested, NestedSchema}, {ForceNonEmpty, Acc}, [{Id, Name} | _]) ->
                NewAcc =
                    case do_random_pathspecs_for_change(ForceNonEmpty, Prob, NestedSchema) of
                        {NewForceNonEmpty, []} ->
                            {NewForceNonEmpty, Acc};
                        {NewForceNonEmpty, NestedPaths} ->
                            Element = {nested, Id, Name, NestedPaths, NestedSchema},
                            {NewForceNonEmpty, [Element | Acc]}
                    end,
                {no_traverse, NewAcc};
            %% HACK: Since we don't know the length of a set, we can't generate random paths for each element that
            %% differ between elements, so the 'change-paths' are the same for all elements
            %% Prop testing gotta work through this anyway though
            ({set, NestedSchema}, {ForceNonEmpty, Acc}, [{Id, Name} | _]) ->
                NewAcc =
                    case do_random_pathspecs_for_change(ForceNonEmpty, Prob, NestedSchema) of
                        {NewForceNonEmpty, []} ->
                            {NewForceNonEmpty, Acc};
                        {NewForceNonEmpty, NestedPaths} ->
                            Element = {set, Id, Name, NestedPaths, NestedSchema},
                            {NewForceNonEmpty, [Element | Acc]}
                    end,
                {no_traverse, NewAcc};
            ({union, DiscriminatorName, UnionSchema}, {ForceNonEmpty, Acc}, [{Id, Name} | _]) ->
                {ForceNonEmpty1, VariantPathSpecs} =
                    lists:foldl(
                        fun({VariantId, VariantSchema}, {ForceNonEmptyAcc, VariantAcc}) ->
                            case do_random_pathspecs_for_change(ForceNonEmptyAcc, Prob, VariantSchema) of
                                {NewForceNonEmptyAcc, []} ->
                                    {NewForceNonEmptyAcc, VariantAcc};
                                {NewForceNonEmptyAcc, VariantPathSpecs} ->
                                    {NewForceNonEmptyAcc, [{VariantId, VariantPathSpecs} | VariantAcc]}
                            end
                        end,
                        {ForceNonEmpty, []},
                        maps:to_list(UnionSchema)
                    ),

                %% Should Discriminator value be changed
                {ForceNonEmpty2, DiscriminatorNamePath} =
                    case ForceNonEmpty1 orelse dice(Prob) of
                        true -> {false, DiscriminatorName};
                        false -> {false, undefined}
                    end,

                PrependElements =
                    case DiscriminatorNamePath == undefined andalso VariantPathSpecs == [] of
                        true -> [];
                        false -> [{union, Id, Name, DiscriminatorNamePath, VariantPathSpecs, UnionSchema}]
                    end,
                {ForceNonEmpty2, PrependElements ++ Acc}
        end,
        {ForceNonEmptyInit, []},
        Schema
    ).

%% Score -- number of fields with changeable values
schema_score(Schema) ->
    traverse_schema(
        fun
            (reserved, Acc, _) ->
                Acc;
            (value, Acc, _) ->
                Acc + 1;
            ({nested, _}, Acc, _) ->
                Acc;
            ({set, _}, Acc, _) ->
                Acc;
            ({union, _, _}, Acc, _) ->
                %% Discriminator can be changed
                Acc + 1
        end,
        0,
        Schema
    ).

random_nonexistent_pathspecs(Schema) ->
    %% TODO: How to generate non-empty ones?
    do_random_nonexistent_pathspecs(Schema).
do_random_nonexistent_pathspecs(Schema) ->
    %% TODO: base on rand number up to schema complexity: total number of nodes and leafs
    traverse_schema(
        fun
            (reserved, Acc, _) ->
                Acc;
            (value, Acc, [{Id, _Name} | _]) ->
                case dice(2) of
                    true ->
                        Name = generate_unique_binary(),
                        [{value, Id, Name} | Acc];
                    false ->
                        Acc
                end;
            ({nested, NestedSchema}, Acc, [{Id, Name} | _]) ->
                NewAcc =
                    case do_random_nonexistent_pathspecs(NestedSchema) of
                        [] ->
                            Acc;
                        NestedPaths ->
                            Element = {nested, Id, Name, NestedPaths, NestedSchema},
                            [Element | Acc]
                    end,
                {no_traverse, NewAcc};
            ({set, _Nested}, Acc, _RevPath) ->
                %% Can't create nonexistent paths for sets: changes natural order of elements => changes them
                {no_traverse, Acc};
            ({union, _DiscriminatorName, UnionSchema}, Acc, [{Id, Name} | _]) ->
                VariantPathSpecs =
                    lists:flatmap(
                        fun({Idx, UnionVariantSchema}) ->
                            case dice(2) of
                                true ->
                                    [{Idx, do_random_nonexistent_pathspecs(UnionVariantSchema)}];
                                false ->
                                    []
                            end
                        end,
                        maps:to_list(UnionSchema)
                    ),

                case VariantPathSpecs of
                    [] ->
                        Acc;
                    _ ->
                        Element = {union, Id, Name, undefined, VariantPathSpecs, UnionSchema},
                        [Element | Acc]
                end
        end,
        [],
        Schema
    ).

change_values_by_paths(Paths, Entity) ->
    lists:foldl(
        fun
            ({value, _Id, Name}, EntityAcc) ->
                NewValue = generate_unique_binary(),
                maps:put(Name, NewValue, EntityAcc);
            ({nested, _Id, Name, NestedPaths, _NestedSchema}, EntityAcc) ->
                maps_update_existing_with(
                    Name,
                    fun(NestedEntity) ->
                        NewNestedEntity = change_values_by_paths(NestedPaths, NestedEntity),
                        NewNestedEntity
                    end,
                    EntityAcc
                );
            ({set, _Id, Name, NestedPaths, NestedSchema}, EntityAcc) ->
                maps_update_existing_with(
                    Name,
                    fun(EntityList) ->
                        change_entity_set(EntityList, NestedPaths, NestedSchema)
                    end,
                    EntityAcc
                );
            ({union, _Id, Name, DiscriminatorName, VariantPathSpecs, _UnionSchema}, EntityAcc) ->
                UnionEntity =
                    case maps:find(Name, EntityAcc) of
                        error when DiscriminatorName == undefined ->
                            undefined;
                        error ->
                            #{};
                        {ok, Value} ->
                            Value
                    end,

                case UnionEntity == undefined of
                    true ->
                        EntityAcc;
                    false ->
                        MaybeChangedUnionEntity =
                            case DiscriminatorName == undefined of
                                true ->
                                    UnionEntity;
                                false ->
                                    NewDiscriminatorValue = generate_unique_binary(),
                                    maps:put(DiscriminatorName, NewDiscriminatorValue, UnionEntity)
                            end,

                        NewUnionEntity =
                            lists:foldl(
                                fun({_UnionId, PathSpecs}, UnionEntityAcc) ->
                                    change_values_by_paths(PathSpecs, UnionEntityAcc)
                                end,
                                MaybeChangedUnionEntity,
                                VariantPathSpecs
                            ),

                        maps:put(Name, NewUnionEntity, EntityAcc)
                end
        end,
        Entity,
        Paths
    ).

change_entity_set(Entities, NestedPaths, NestedSchema) ->
    Entities1 = set_maybe_change(Entities, NestedPaths),
    Entities2 = set_maybe_permute(Entities1),
    Entities3 = set_maybe_remove(Entities2),
    _NewEntities = set_maybe_add(Entities3, NestedSchema),
    NewEntities = Entities1,
    %% TODO: move order to assert_correct_compare
    case lists:sort(NewEntities) == lists:sort(Entities) of
        false ->
            NewEntities;
        true ->
            change_entity_set(Entities, NestedPaths, NestedSchema)
    end.

set_maybe_change(Entities, Paths) ->
    case rand:uniform(2) of
        1 ->
            Entities;
        2 ->
            lists:map(
                fun(Entity) ->
                    case rand:uniform(4) of
                        4 -> change_values_by_paths(Paths, Entity);
                        _ -> Entity
                    end
                end,
                Entities
            )
    end.
set_maybe_permute(Entities) ->
    case dice(2) of
        false ->
            Entities;
        true ->
            shuffle(Entities)
    end.
set_maybe_remove(Entities) ->
    case dice(2) of
        false ->
            Entities;
        true ->
            lists:flatmap(
                fun(Entity) ->
                    case rand:uniform(2) of
                        1 -> [Entity];
                        2 -> []
                    end
                end,
                Entities
            )
    end.
set_maybe_add(Entities, Schema) ->
    case dice(2) of
        false ->
            Entities;
        true ->
            Length = length(Entities),
            MaxAddPerIter = max(1, Length),
            Chance = max(1, Length div 2),
            Mapper = fun(Entity) ->
                case dice(Chance) of
                    true ->
                        NewEntities = generate_new_entities(rand1(MaxAddPerIter), Schema),
                        Prepend = dice(2),
                        case Prepend of
                            true ->
                                NewEntities ++ [Entity];
                            false ->
                                [Entity | NewEntities]
                        end;
                    false ->
                        [Entity]
                end
            end,
            lists:flatmap(Mapper, Entities)
    end.

generate_new_entities(Count, Schema) ->
    lists:map(
        fun(_) -> fill_schema(Schema) end,
        lists:seq(1, Count)
    ).

maps_update_existing_with(Key, Fun, Map) ->
    case maps:find(Key, Map) of
        {ok, Value} ->
            maps:put(Key, Fun(Value), Map);
        error ->
            Map
    end.

lists_split_by_perc(Perc, List) ->
    Count = trunc(length(List) * Perc),
    lists:split(Count, List).

-spec lists_split_randomly(PartCount :: pos_integer(), list(T)) -> list(list(T)) when T :: term().
lists_split_randomly(0, _) ->
    [];
lists_split_randomly(PartCount, List) when PartCount > 0 ->
    {Parts, []} = lists:mapfoldl(fun lists:split/2, List, random_partition(PartCount, length(List))),
    Parts.

random_partition(0, _) ->
    [];
random_partition(1, Length) ->
    [Length];
random_partition(Parts, Length) ->
    Cut = rand(Length),
    LeftLength = Cut,
    RightLength = Length - Cut,
    LeftParts = Parts div 2,
    RightParts = Parts - LeftParts,
    random_partition(LeftParts, LeftLength) ++ random_partition(RightParts, RightLength).

rand_elem(List) when List /= [] ->
    lists:nth(rand:uniform(length(List)), List).

dice(Probability) when is_float(Probability) ->
    rand:uniform() =< Probability;
dice(Chance) when is_integer(Chance) ->
    Chance == rand:uniform(Chance).

randf() ->
    rand:uniform().
randf(Limit) when is_float(Limit), Limit >= 0.0, Limit =< 1.0 ->
    Limit * randf().
rand(Limit) ->
    rand1(Limit + 1) - 1.
rand1(Limit) ->
    rand:uniform(Limit).

shuffle(List) ->
    order_by(fun(_Elt) -> randf() end, List).

order_by(Fun, Elements) ->
    lists:map(
        fun({_RandIdx, Element}) -> Element end,
        lists:keysort(
            1,
            lists:map(
                fun(Element) ->
                    {Fun(Element), Element}
                end,
                Elements
            )
        )
    ).

generate_unique_binary() ->
    erlang:list_to_binary(erlang:ref_to_list(make_ref())).

%% "Force" because any encountered value that's not a map is overwritten
%% Doesn't work the other way: map is not rewritten with value
deep_force_put(Keys, Value, Map) when Keys /= [] ->
    do_deep_force_put(Map, Keys, Value, []).

do_deep_force_put(Map, [Key], Value, _Path) ->
    case maps:find(Key, Map) of
        {ok, M} when is_map(M) ->
            {error, map_overwrite};
        _ ->
            {ok, maps:put(Key, Value, Map)}
    end;
do_deep_force_put(Map, [Key | Rest], Value, Path) ->
    NestedMap =
        case maps:find(Key, Map) of
            {ok, NM} when is_map(NM) ->
                NM;
            _ ->
                #{}
        end,
    case do_deep_force_put(NestedMap, Rest, Value, [Key | Path]) of
        {ok, NewNestedMap} ->
            {ok, maps:put(Key, NewNestedMap, Map)};
        {error, _} = Error ->
            Error
    end.

deep_fetch(Keys, Map) ->
    do_deep_fetch(Map, Keys, []).

do_deep_fetch(Value, [], _Path) ->
    {ok, Value};
do_deep_fetch(NotMap, [_Key | _Rest], Path) when not is_map(NotMap) ->
    {error, not_map, Path};
do_deep_fetch(Map, [Key | Rest], Path) ->
    NewPath = [Key | Path],
    case maps:find(Key, Map) of
        error ->
            {error, not_found, NewPath};
        {ok, NextValue} ->
            do_deep_fetch(NextValue, Rest, NewPath)
    end.

id_path(Path) ->
    lists:map(fun({Id, _Name}) -> Id end, Path).

name_path(Path) ->
    lists:map(fun({_Id, Name}) -> Name end, Path).

pathspecs_to_binpaths(PathSpecs, Diff) ->
    lists:flatmap(
        fun(PathSpec) ->
            lists:flatmap(
                fun(PathList) ->
                    case lists:join($., PathList) of
                        [] -> [];
                        PathSegments -> [list_to_binary(PathSegments)]
                    end
                end,
                do_pathspec_to_binpath(PathSpec, Diff)
            )
        end,
        PathSpecs
    ).

do_pathspec_to_binpath({value, Id, Name}, Diff) ->
    case maps:find(Id, Diff) of
        {ok, ?difference} -> [[Name]];
        _ -> []
    end;
do_pathspec_to_binpath({nested, Id, Name, NestedPaths, _NestedSchema}, Diff) ->
    case maps:get(Id, Diff) of
        ?difference ->
            [[Name]];
        NestedDiff ->
            lists:flatmap(
                fun(PathSpec) ->
                    lists:map(
                        fun(NextPath) ->
                            [Name | NextPath]
                        end,
                        do_pathspec_to_binpath(PathSpec, NestedDiff)
                    )
                end,
                NestedPaths
            )
    end;
do_pathspec_to_binpath({set, Id, Name, _NestedPaths, NestedSchema}, Diff) ->
    case maps:get(Id, Diff) of
        ?difference ->
            [[Name]];
        SetDiff ->
            %% For each element of set
            lists:flatmap(
                fun
                    ({Index, ?difference}) ->
                        [[Name, erlang:integer_to_binary(Index)]];
                    ({Index, NestedDiff}) ->
                        %% For each (changed) path
                        %% BUG: Natural reordering due to changes to multiple fields is not taken into account
                        lists:flatmap(
                            fun(PathSpec) ->
                                lists:map(
                                    fun(NextPath) ->
                                        [Name, erlang:integer_to_binary(Index) | NextPath]
                                    end,
                                    do_pathspec_to_binpath(PathSpec, NestedDiff)
                                )
                            end,
                            pathspecs(NestedSchema)
                        )
                end,
                maps:to_list(SetDiff)
            )
    end;
do_pathspec_to_binpath({union, Id, Name, _DiscriminatorName, VariantPathSpecs, _UnionSchema}, Diff) ->
    case maps:find(Id, Diff) of
        error ->
            [];
        {ok, ?difference} ->
            [[Name]];
        {ok, UnionDiff} ->
            lists:flatmap(
                fun({VariantId, VariantSchema}) ->
                    case maps:get(VariantId, UnionDiff) of
                        ?difference ->
                            [[]];
                        VariantDiff ->
                            lists:map(
                                fun(NextPath) -> [Name, NextPath] end,
                                do_pathspec_to_binpath(VariantSchema, VariantDiff)
                            )
                    end
                end,
                maps:to_list(VariantPathSpecs)
            )
    end.
