-module(prop_feat).

-include("feat.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(PROP(Prop), ?PROP(Prop, [])).
-define(PROP(Prop, Opts), ?assert(proper:quickcheck(Prop, [{to_file, user}] ++ Opts))).

-define(RAND_ALG, exsss).
-define(SET_SEED(Seed), _ = rand:seed(?RAND_ALG, Seed)).

-spec prop_hash_calculatable() -> proper:test().
prop_hash_calculatable() ->
    ?FORALL(Term, term(), is_integer(feat:hash(Term))).

%% TODO:
%% 1. prop_compare_scrambled_sets
%% 2. Invalid schema (e.g. #{0 => [<<"0">>],1 => [<<"0">>]})
%% 3. Speed up tests (stack for some (almost final) cases)
%% 4. EUnit-embed for schema validation: ?FEAT_CHECK_SCHEMAS(SchemaOrSchemas) and ?assertValidFeatureSchema(Schema)
-spec prop_read() -> proper:test().
prop_read() ->
    ?FORALL(
        Schema,
        schema(),
        begin
            %% io:fwrite("~p~n", [[{schema, Schema}]]),
            Entity = fill_schema(Schema),
            Features = feat:read(Schema, Entity),
            %% io:fwrite("~p~n", [[{'entity', Entity}]]),
            %% io:fwrite("~p~n", [[{features, Features}]]),

            is_map(Features) andalso
                assert_correct_read(Schema, Features, Entity)
        end
    ).

-spec prop_compare_same() -> proper:test().
prop_compare_same() ->
    ?FORALL(
        [
            Schema,
            Seed
        ],
        [
            schema(),
            integer()
        ],
        begin
            ?SET_SEED(Seed),

            Entity1 = fill_schema(Schema),
            PathSpecs = random_nonexistent_pathspecs(Schema),
            %% io:fwrite("~p~n", ["~n"]),
            %% io:fwrite("~p~n", [[{schema, Schema}]]),
            %% io:fwrite("~p~n", [[{paths, PathSpecs}]]),
            %% io:fwrite("~p~n", [[{'entity1', Entity1}]]),

            Entity2 = change_values_by_paths(PathSpecs, Entity1),

            %% io:fwrite("~p~n", [[{'entity2', Entity2}]]),

            Features1 = feat:read(Schema, Entity1),
            Features2 = feat:read(Schema, Entity2),
            %% io:fwrite("~p~n", [{'features1', Features1}]),
            %% io:fwrite("~p~n", [{'features2', Features2}]),
            %% io:fwrite("~p~n", [{diff, feat:compare(Features1, Features2)}]),

            ?assertEqual(true, feat:compare(Features1, Features2)),
            true
        end
    ).

-spec prop_compare_different() -> proper:test().
prop_compare_different() ->
    ?FORALL(
        [Schema, Seed],
        [schema(), integer()],
        begin
            ?SET_SEED(Seed),
            Entity1 = fill_schema(Schema),

            %% TODO: move to such that
            %% ?assertNotEqual(Entity, Entity2),
            case random_pathspecs_for_change(Schema) of
                [] ->
                    true;
                PathSpecs ->
                    %% io:fwrite("~p~n", [{schema, Schema}]),
                    %% io:fwrite("~p~n", [{paths, PathSpecs}]),
                    %% io:fwrite("~p~n", [{first, Entity1}]),
                    Entity2 = change_values_by_paths(PathSpecs, Entity1),
                    %% io:fwrite("~p~n", [{second, Entity2}]),

                    Features1 = feat:read(Schema, Entity1),
                    Features2 = feat:read(Schema, Entity2),

                    {false, Diff} = feat:compare(Features1, Features2),
                    %% io:fwrite("~p~n", [{diff, Diff}]),

                    is_map(Diff) andalso
                        assert_correct_compare(Diff, PathSpecs)
            end
        end
    ).

-spec prop_list_diff_fields() -> proper:test().
prop_list_diff_fields() ->
    ?FORALL(
        [Schema, Seed],
        [schema(), integer()],
        begin
            ?SET_SEED(Seed),
            %% io:fwrite("~p~n", [{schema, Schema}]),

            Entity1 = fill_schema(Schema),

            %% TODO: move to such that
            %% ?assertNotEqual(Entity, Entity2),
            case random_pathspecs_for_change(Schema) of
                [] ->
                    true;
                PathSpecs ->
                    %% io:fwrite("~p~n", [{seed, Seed}]),
                    %% io:fwrite("~p~n", [{first, Entity1}]),
                    %% io:fwrite("~p~n", [{paths, PathSpecs}]),

                    Entity2 = change_values_by_paths(PathSpecs, Entity1),
                    %% io:fwrite("~p~n", [{second, Entity2}]),

                    Features1 = feat:read(Schema, Entity1),
                    Features2 = feat:read(Schema, Entity2),

                    {false, Diff} = feat:compare(Features1, Features2),
                    %% io:fwrite("~p~n", [{diff, Diff}]),

                    DiffFields = feat:list_diff_fields(Schema, Diff),
                    %% io:fwrite("~p~n", [{diff_fields, DiffFields}]),

                    ChangedFields =
                        pathspecs_to_binpaths(PathSpecs, Diff),

                    %% io:fwrite("~p~n", [{fields, ChangedFields}]),

                    is_map(Diff) andalso
                        assertEqualSets(ChangedFields, DiffFields)
            end
        end
    ).

schema() ->
    schema(#{avg_depth => 3}).

schema(Opts) ->
    ?LET(
        Features,
        features(),
        generate_schema(Opts, Features)
    ).

generate_schema(Opts, Features) ->
    {_, SchemaAcc} = do_generate_schema(Opts, Features, #{}),
    SchemaAcc.

%% TODO: remove RandState
do_generate_schema(_Opts, [], Acc) ->
    {[], Acc};
do_generate_schema(Opts, [{FeatureID, FeatureName} | RestFeatures], Acc) ->
    AvgDepth = maps:get(avg_depth, Opts, 3),

    %% Dice: 1 means simple field, 2 ­ Union nested schema (with discriminator), >1 ­ nested schema
    DiceNested = rand:uniform(AvgDepth + 1),
    %% DiceValueKind = rand:uniform(2),
    DiceUnion = rand:uniform(2),
    %% TODO: add sets
    {NextFeatures, NextAcc} =
        if
            %% SetSchema
            %% NestedSchema (inc. set)
            DiceNested > 1, DiceUnion == 1, tl(RestFeatures) /= [] ->
                {LeftFeatures, NestedSchema} = do_generate_schema(
                    Opts,
                    RestFeatures,
                    #{}
                ),
                DiceValueKind = rand:uniform(2),
                Value =
                    case DiceValueKind of
                        1 -> NestedSchema;
                        2 -> {set, NestedSchema}
                    end,
                {LeftFeatures, maps:put(FeatureID, [FeatureName, Value], Acc)};
            %% DiscriminatedSchema aka union
            DiceNested > 1, DiceUnion == 2, tl(RestFeatures) /= [] ->
                MaxUnionWidth = min(
                    maps:get(max_union_width, Opts, 3),
                    length(RestFeatures)
                ),
                UnionWidth = rand(MaxUnionWidth),

                {UnionElementsFeatures, RestFeatures1} = lists:split(UnionWidth, RestFeatures),

                MaxUnionElements = maps:get(max_union_elements, Opts, 7),

                %% TODO: Can't features be reused across union elements? Or it's schema error (check during tests)
                {UnionSchema, NextRestFeatures} =
                    lists:foldl(
                        fun({ElementFeatureID, _ElementFeatureName}, {UnionAcc, CurrentRestFeatures}) ->
                            MaxNestedFeaturesCount = rand(min(MaxUnionElements, length(CurrentRestFeatures))),

                            {UsedNestedFeatures, NextRestFeatures} =
                                lists:split(rand(MaxNestedFeaturesCount), CurrentRestFeatures),

                            {_, NestedSchema} =
                                do_generate_schema(
                                    Opts,
                                    UsedNestedFeatures,
                                    #{}
                                ),

                            %% TODO: is it valid schema
                            NextUnionAcc =
                                case map_size(NestedSchema) of
                                    0 -> UnionAcc;
                                    _ -> maps:put(ElementFeatureID, NestedSchema, UnionAcc)
                                end,

                            {NextUnionAcc, NextRestFeatures}
                        end,
                        {#{
                                ?discriminator => [FeatureName]
                            },
                            RestFeatures1},
                        UnionElementsFeatures
                    ),

                {NextRestFeatures, maps:put(FeatureID, [FeatureName, UnionSchema], Acc)};
            true ->
                {RestFeatures, maps:put(FeatureID, [FeatureName], Acc)}
        end,
    do_generate_schema(Opts, NextFeatures, NextAcc).

features() ->
    ?LET(
        Names,
        ?SUCHTHAT(
            Names,
            list(identifier()),
            length(Names) == length(sets:to_list(sets:from_list(Names)))
        ),
        lists:zip(lists:seq(1, length(Names)), Names)
    ).

%% non_unique_features() ->
%%     list({non_neg_integer(), identifier()}).

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

%% Probably useful functions for ?SUCHTHAT

%% schema_depth(Map) ->
%%     do_schema_depth(Map, 1).

%% do_schema_depth(Map, Depth) ->
%%     traverse_schema(
%%         fun
%%             ({nested, NestedSchema}, MaxDepth, _RevPath) ->
%%                 max(MaxDepth, do_schema_depth(NestedSchema, Depth + 1));
%%             (_, MaxDepth, _) ->
%%                 MaxDepth
%%         end,
%%         Depth,
%%         Map
%%     ).

%% schema_width(Map) ->
%%     do_schema_width(Map, map_size(Map)).
%% do_schema_width(Map, Width) ->
%%     traverse_schema(
%%         fun
%%             ({nested, NestedSchema}, MaxWidth, _RevPath) ->
%%                 max(MaxWidth, schema_width(NestedSchema));
%%             (_, MaxWidth, _RevPath) ->
%%                 MaxWidth
%%         end,
%%         Width,
%%         Map
%%     ).

fill_schema(Schema) ->
    traverse_schema(
        fun
            (value, Acc, RevPath) ->
                %% io:fwrite("~p~n", [{value, lists:reverse(RevPath)}]),
                %% TODO: applicable term()s generation
                %% %% TODO?: sparsity (how many are not defined)
                Value = generate_unique_binary(),

                NamePath = name_path(lists:reverse(RevPath)),
                case deep_force_put(NamePath, Value, Acc) of
                    {ok, NewAcc} -> NewAcc;
                    {error, map_overwrite} -> Acc
                end;
            ({set, NestedSchema}, Acc, RevPath) ->
                %% io:fwrite("~p~n", [{set, lists:reverse(RevPath)}]),
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
                UnionElement =
                    case maps:to_list(UnionSchema) of
                        [] ->
                            #{};
                        UnionElementsSchemas ->
                            {_Idx, UnionElementSchema} = rand_elem(UnionElementsSchemas),
                            fill_schema(UnionElementSchema)
                    end,

                DiscriminatorValue = generate_unique_binary(),
                Value = maps:put(DiscriminatorName, DiscriminatorValue, UnionElement),

                NamePath = name_path(lists:reverse(RevPath)),
                NextAcc =
                    case deep_force_put(NamePath, Value, Acc) of
                        {ok, NewAcc} -> NewAcc;
                        {error, map_overwrite} -> Acc
                    end,
                NextAcc;
            (_, Acc, _RevPath) ->
                Acc
        end,
        #{},
        Schema
    ).

assert_correct_read(Schema, Features, Entity) ->
    %% io:fwrite("~p~n", [{schema, Schema}]),
    %% io:fwrite("~p~n", [{features, Features}]),
    %% io:fwrite("~p~n", [{entity, Entity}]),
    traverse_schema(
        fun
            (_, false, _) ->
                false;
            ({nested, _}, Acc, _) ->
                Acc;
            (Kind, true, RevPath) ->
                Path = lists:reverse(RevPath),
                %% io:fwrite("~p~n", [{path, Path}]),
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
                        %% io:fwrite("~p~n", [{set, NestedSchema, NestedFeatureSet, NestedValues}]),
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
                        DiscriminatorResult =
                            maps:get(?discriminator, NestedFeatures) ==
                                feat:hash(maps:get(DiscriminatorName, NestedValues)),

                        OkUnionElementsCount =
                            maps:fold(
                                fun(Idx, UnionElementSchema, Acc) ->
                                    UnionElementFeatures =
                                        maps:get(Idx, NestedFeatures),

                                    try
                                        assert_correct_read(
                                            UnionElementSchema,
                                            UnionElementFeatures,
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
                        UnionWidth = map_size(UnionSchema),
                        UnionElementsResult = (OkUnionElementsCount > 0) or (UnionWidth == 0),

                        DiscriminatorResult and UnionElementsResult;
                    {_, {nested, _, _}} ->
                        throw({nested_features, Path})
                end
        end,
        true,
        Schema
    ).

assert_correct_compare(Diff, Paths) ->
    do_assert_correct_compare(Diff, Paths, []).

%% TODO: check that all paths are different?
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
                    error ->
                        logger:error("Nested diff is not found at ~p for subschema: ~p~n", [
                            lists:reverse(NewRevPath),
                            NestedSchema
                        ]),
                        false
                end;
            ({set, Id, Name, NestedPaths, NestedSchema}, _) ->
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
                end;
            ({union, Id, Name, DiscriminatorName, ElementPathSpecs, UnionSchema}, _) ->
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
                                        {ok, UnionElementDiff} ->
                                            do_assert_correct_compare(
                                                UnionElementDiff,
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
                            ElementPathSpecs
                        );
                    error ->
                        logger:error("Union diff is not found at ~p for subschema: ~p~n", [
                            lists:reverse(NewRevPath),
                            UnionSchema
                        ]),
                        false
                end
        end,
        true,
        Paths
    ).

pathspecs(Schema) ->
    traverse_schema(
        fun
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
                ElementPathSpecs =
                    maps:to_list(
                        maps:map(
                            fun(_ElementId, NestedSchema) -> pathspecs(NestedSchema) end,
                            UnionSchema
                        )
                    ),

                Element = {union, Id, Name, DiscriminatorName, ElementPathSpecs, UnionSchema},
                [Element | Acc]
        end,
        [],
        Schema
    ).
random_pathspecs_for_change(Schema) ->
    traverse_schema(
        fun
            (value, Acc, [{Id, Name} | _]) ->
                case rand:uniform(2) of
                    1 ->
                        [{value, Id, Name} | Acc];
                    2 ->
                        Acc
                end;
            ({nested, NestedSchema}, Acc, [{Id, Name} | _]) ->
                NewAcc =
                    case random_pathspecs_for_change(NestedSchema) of
                        [] ->
                            Acc;
                        NestedPaths ->
                            Element = {nested, Id, Name, NestedPaths, NestedSchema},
                            [Element | Acc]
                    end,
                {no_traverse, NewAcc};
            %% HACK: Since we don't know the length of a set, we can't generate random paths for each element that
            %% differ between elements, so the 'change-paths' are the same for all elements
            %% Prop testing gotta work through this anyway though
            ({set, NestedSchema}, Acc, [{Id, Name} | _]) ->
                NewAcc =
                    case rand:uniform(2) of
                        1 ->
                            NestedPaths = random_pathspecs_for_change(NestedSchema),
                            Element = {set, Id, Name, NestedPaths, NestedSchema},
                            [Element | Acc];
                        2 ->
                            Acc
                    end,
                {no_traverse, NewAcc};
            ({union, DiscriminatorName, UnionSchema}, Acc, [{Id, Name} | _]) ->
                ElementPathSpecs =
                    lists:flatmap(
                        fun({ElementId, NestedSchema}) ->
                            case dice(2) of
                                false -> [];
                                true -> [{ElementId, random_pathspecs_for_change(NestedSchema)}]
                            end
                        end,
                        maps:to_list(UnionSchema)
                    ),
                %% Should Discriminator value be changed
                DiscriminatorNamePath =
                    case dice(2) of
                        true -> DiscriminatorName;
                        false -> undefined
                    end,

                PrependElements =
                    case DiscriminatorNamePath == undefined orelse ElementPathSpecs == [] of
                        true -> [];
                        false -> [{union, Id, Name, DiscriminatorNamePath, ElementPathSpecs, UnionSchema}]
                    end,
                PrependElements ++ Acc
        end,
        [],
        Schema
    ).

random_nonexistent_pathspecs(Schema) ->
    traverse_schema(
        fun
            (value, Acc, [{Id, _Name} | _]) ->
                case rand:uniform(2) of
                    1 ->
                        Name = generate_unique_binary(),
                        [{value, Id, Name} | Acc];
                    2 ->
                        Acc
                end;
            ({nested, NestedSchema}, Acc, [{Id, Name} | _]) ->
                NewAcc =
                    case random_nonexistent_pathspecs(NestedSchema) of
                        [] ->
                            Acc;
                        NestedPaths ->
                            Element = {nested, Id, Name, NestedPaths, NestedSchema},
                            [Element | Acc]
                    end,
                {no_traverse, NewAcc};
            ({set, _Nested}, Acc, _RevPath) ->
                %% Can't create nonexistent paths for sets
                {no_traverse, Acc};
            ({union, _DiscriminatorName, UnionSchema}, Acc, [{Id, Name} | _]) ->
                ElementPathSpecs =
                    lists:flatmap(
                        fun({Idx, UnionElementSchema}) ->
                            case dice(2) of
                                true ->
                                    [{Idx, random_nonexistent_pathspecs(UnionElementSchema)}];
                                false ->
                                    []
                            end
                        end,
                        maps:to_list(UnionSchema)
                    ),

                case ElementPathSpecs of
                    [] ->
                        Acc;
                    _ ->
                        Element = {union, Id, Name, undefined, ElementPathSpecs, UnionSchema},
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
                        %% io:fwrite("~p~n", [{change, NestedEntity, NewNestedEntity, NestedPaths}]),
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
            ({union, _Id, Name, DiscriminatorName, ElementPathSpecs, _UnionSchema}, EntityAcc) ->
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
                                ElementPathSpecs
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
    NewEntities = set_maybe_add(Entities3, NestedSchema),
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
            %% io:fwrite("~p~n", ["Change"]),
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
    case rand:uniform(2) of
        1 ->
            Entities;
        2 ->
            %% io:fwrite("~p~n", ["Permute"]),
            scramble(Entities)
    end.
set_maybe_remove(Entities) ->
    case rand:uniform(2) of
        1 ->
            Entities;
        2 ->
            %% io:fwrite("~p~n", ["Remove"]),
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
    case rand:uniform(2) of
        1 ->
            Entities;
        2 ->
            Length = length(Entities),
            MaxAddPerIter = max(1, Length),
            Chance = max(1, Length div 2),
            lists:flatmap(
                fun(Entity) ->
                    case {rand:uniform(Chance), rand:uniform(2), rand:uniform(MaxAddPerIter)} of
                        {Value, Where, Count} when Value == Chance ->
                            NewEntities =
                                lists:map(
                                    fun(_) -> fill_schema(Schema) end,
                                    lists:seq(1, Count)
                                ),
                            case Where of
                                1 ->
                                    NewEntities ++ [Entity];
                                2 ->
                                    [Entity | NewEntities]
                            end;
                        _ ->
                            [Entity]
                    end
                end,
                Entities
            )
    end.

maps_update_existing_with(Key, Fun, Map) ->
    case maps:find(Key, Map) of
        {ok, Value} ->
            maps:put(Key, Fun(Value), Map);
        error ->
            Map
    end.

rand_elem(List) when List /= [] ->
    lists:nth(rand:uniform(length(List)), List).

dice(Chance) ->
    Chance == rand:uniform(Chance).

rand(Limit) ->
    rand:uniform(Limit + 1) - 1.

scramble(List) ->
    order_by(fun(_Elt) -> rand:uniform() end, List).

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

assertEqualSets(List1, List2) ->
    ?assertEqual(List1 -- List2, List2 -- List1),
    true.

id_path(Path) ->
    lists:map(fun({Id, _Name}) -> Id end, Path).

name_path(Path) ->
    lists:map(fun({_Id, Name}) -> Name end, Path).

pathspecs_to_binpaths(PathSpecs, Diff) ->
    lists:flatmap(
        fun(PathSpec) ->
            %% io:fwrite("~p~n", [{pathspec, PathSpec}]),
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
    case maps:get(Id, Diff, undefined) of
        ?difference -> [[Name]];
        undefined -> []
    end;
do_pathspec_to_binpath({nested, Id, Name, NestedPaths, _NestedSchema}, Diff) ->
    lists:flatmap(
        fun(PathSpec) ->
            lists:map(
                fun(NextPath) ->
                    [Name | NextPath]
                end,

                do_pathspec_to_binpath(PathSpec, maps:get(Id, Diff, #{}))
            )
        end,
        NestedPaths
    );
%% TODO: wtf is this? rewrite
do_pathspec_to_binpath({set, Id, Name, _NestedPaths, NestedSchema}, Diff) ->
    case maps:get(Id, Diff) of
        ?difference ->
            [[Name]];
        SetDiff ->
            PathSpecs = pathspecs(NestedSchema),
            %% For each element of set
            lists:flatmap(
                fun({Index, NestedDiff}) ->
                    %%
                    lists:flatmap(
                        fun(PathSpec) ->
                            %% io:fwrite("~p~n", [{spec, PathSpec}]),
                            lists:map(
                                fun(NextPath) ->
                                    [Name, erlang:integer_to_binary(Index) | NextPath]
                                end,
                                do_pathspec_to_binpath(PathSpec, NestedDiff)
                            )
                        end,
                        PathSpecs
                    )
                end,
                maps:to_list(SetDiff)
            )
    end;
do_pathspec_to_binpath({union, Id, Name, _DiscriminatorName, ElementPathSpecs, _UnionSchema}, Diff) ->
    case maps:get(Id, Diff) of
        ?difference ->
            [[Name]];
        UnionDiff ->
            lists:flatmap(
                fun({ElementId, ElementSchema}) ->
                    ElementDiff = maps:get(ElementId, UnionDiff),
                    lists:map(
                        fun(NextPath) -> [Name, NextPath] end,
                        do_pathspec_to_binpath(ElementSchema, ElementDiff)
                    )
                end,
                maps:to_list(ElementPathSpecs)
            )
    end.

traverse_schema(Fun, Acc, Schema) ->
    {ResultAcc, _RevPath} = do_traverse_schema(Fun, Acc, Schema, []),
    ResultAcc.

do_traverse_schema(Fun, Acc, Schema, InitRevPath) ->
    maps:fold(
        fun
            (Id, [Name, UnionSchema = #{?discriminator := [DiscriminatorName]}], {CurrentAcc, RevPath}) ->
                NewAcc = Fun({union, DiscriminatorName, maps:remove(?discriminator, UnionSchema)}, CurrentAcc, [
                    {Id, Name}
                    | RevPath
                ]),
                %% TODO: no_traverse here?
                {NewAcc, RevPath};
            (Id, [Name, Value], {CurrentAcc, RevPath}) ->
                NewRevPath = [{Id, Name} | RevPath],
                Arg =
                    {_, NestedSchema} =
                    case Value of
                        {set, Nested} ->
                            {set, Nested};
                        Nested ->
                            {nested, Nested}
                    end,
                ResultAcc =
                    case Fun(Arg, CurrentAcc, NewRevPath) of
                        {no_traverse, ReturnedAcc} ->
                            ReturnedAcc;
                        NewAcc ->
                            {ReturnedAcc, _RevPath} = do_traverse_schema(Fun, NewAcc, NestedSchema, NewRevPath),
                            ReturnedAcc
                    end,
                {ResultAcc, RevPath};
            (Id, [Name], {CurrentAcc, RevPath}) ->
                NewAcc = Fun(value, CurrentAcc, [{Id, Name} | RevPath]),
                {NewAcc, RevPath}
        end,
        {Acc, InitRevPath},
        Schema
    ).
