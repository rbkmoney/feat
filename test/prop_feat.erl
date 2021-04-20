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
-spec prop_read() -> proper:test().
prop_read() ->
    ?FORALL(
        Schema,
        schema(),
        begin
            Entity = fill_schema(Schema),
            Features = feat:read(Schema, Entity),

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

            %% logger:info("~n"),
            %% logger:info("~p", [{schema, Schema}]),
            %% logger:info("~p", [{paths, PathSpecs}]),
            %% logger:info("~p", [{'entity1', Entity1}]),
            %% logger:info("~p", [{'entity2', Entity2}]),
            %% logger:info("~p", [{'features1', Features1}]),
            %% logger:info("~p", [{'features2', Features2}]),
            %% io:fwrite("~p~n", [{'diff', feat:compare(Features1, Features2)}]),

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
            case random_pathspecs(Schema) of
                [] ->
                    true;
                PathSpecs ->
                    Entity2 = change_values_by_paths(PathSpecs, Entity1),

                    Features1 = feat:read(Schema, Entity1),
                    Features2 = feat:read(Schema, Entity2),

                    {false, Diff} = feat:compare(Features1, Features2),

                    %% io:fwrite("~p~n", [{schema, Schema}]),
                    %% io:fwrite("~p~n", [{paths, PathSpecs}]),
                    %% io:fwrite("~p~n", [{first, Entity1}]),
                    %% io:fwrite("~p~n", [{second, Entity2}]),
                    %% io:fwrite("~p~n", [{diff, Diff}]),

                    is_map(Diff) andalso
                        assert_correct_compare(Diff, PathSpecs, Entity1, Entity2)
            end
        end
    ).

-spec prop_list_diff_fields() -> proper:test().
prop_list_diff_fields() ->
    ?FORALL(
        [Schema, Seed],
        [schema(), integer()],
        begin
            %% Seed = 4,
            %% Schema = #{1 => [<<"1">>],
            %%            2 =>
            %%                [<<"2">>,
            %%                 {set,#{3 =>
            %%                            [<<"3">>,
            %%                             #{4 => [<<"4">>],
            %%                               5 => [<<"5">>],
            %%                               6 => [<<"6">>],
            %%                               7 => [<<"7">>]}]}}]},
            %% Schema = #{0 =>
            %%                [<<"0">>,
            %%                 #{1 => [<<"00">>],
            %%                   2 =>
            %%                       [<<"1">>,
            %%                        {set,#{3 => [<<"01">>],
            %%                               4 => [<<"3">>],
            %%                               5 => [<<"4">>],
            %%                               6 => [<<"5">>],
            %%                               7 =>
            %%                                   [<<"2">>,
            %%                                    #{8 => [<<"6">>],
            %%                                      9 => [<<"A">>],
            %%                                      10 => [<<"B">>],
            %%                                      11 => [<<"C">>]}]}}]}]} ,
            %% Schema = #{0 => [<<48>>, {set, #{1 => [<<49>>], 2 => [<<50>>]}}]},
            %% Seed = -7,
            ?SET_SEED(Seed),

            Entity1 = fill_schema(Schema),

            %% TODO: move to such that
            %% ?assertNotEqual(Entity, Entity2),
            case random_pathspecs(Schema) of
                [] ->
                    true;
                PathSpecs ->
                    Entity2 = change_values_by_paths(PathSpecs, Entity1),

                    Features1 = feat:read(Schema, Entity1),
                    Features2 = feat:read(Schema, Entity2),

                    {false, Diff} = feat:compare(Features1, Features2),

                    DiffFields = feat:list_diff_fields(Schema, Diff),

                    %% io:fwrite("~p~n", [{seed, Seed}]),
                    %% io:fwrite("~p~n", [{schema, Schema}]),
                    %% io:fwrite("~p~n", [{paths, PathSpecs}]),
                    %% io:fwrite("~p~n", [{first, Entity1}]),
                    %% io:fwrite("~p~n", [{second, Entity2}]),
                    %% io:fwrite("~p~n", [{diff, Diff}]),
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
    schema(3).

schema(AvgDepth) ->
    ?LET(
        [Seed, Features],
        [integer(), features()],
        build_schema(AvgDepth, Features, rand:seed_s(?RAND_ALG, Seed))
    ).

build_schema(AvgDepth, Features, RandState) ->
    {_, _, SchemaAcc} = do_build_schema(AvgDepth, Features, RandState, #{}),
    SchemaAcc.

do_build_schema(_AvgDepth, [], RandState, Acc) ->
    {[], RandState, Acc};
do_build_schema(AvgDepth, [{FeatureID, FeatureName} | RestFeatures], RandState, Acc) ->
    %% Dice: 1 means simple field, 2 ­ Union nested schema (with discriminator), >1 ­ nested schema
    {DiceNested, RandState1} = rand:uniform_s(AvgDepth + 1, RandState),
    %% {DiceValueKind, RandState2} = rand:uniform_s(2, RandState1),
    {DiceUnion, RandState2} = rand:uniform_s(2, RandState1),
    %% TODO: add sets
    {NextFeatures, NextRandState, NextAcc} =
        if
            %% SetSchema
            %% NestedSchema (inc. set)
            DiceNested > 1, DiceUnion == 1, tl(RestFeatures) /= [] ->
                {LeftFeatures, RandState3, NestedSchema} = do_build_schema(
                    AvgDepth,
                    RestFeatures,
                    RandState2,
                    #{}
                ),
                {DiceValueKind, RandState4} = rand:uniform_s(2, RandState3),
                Value =
                    case DiceValueKind of
                        1 -> NestedSchema;
                        2 -> {set, NestedSchema}
                    end,
                {LeftFeatures, RandState4, maps:put(FeatureID, [FeatureName, Value], Acc)};
            %% %% DiscriminatedSchema
            %% DiceNested > 1, tl(RestFeatures) /= [] ->
            %%     {LeftFeatures, ReturnedRandState, NestedSchema} = do_build_schema(
            %%                                                         AvgDepth,
            %%                                                         RestFeatures,
            %%                                                         NewRandState,
            %%                                                         #{}
            %%                                                        ),
            %%     {LeftFeatures, ReturnedRandState, maps:put(FeatureID, [FeatureName, NestedSchema], Acc)};
            %% SetSchema
            %% DiceValueKind == 2 ->
            %%     {RestFeatures, NewRandState, maps:put(FeatureID, [FeatureName], Acc)} ;
            true ->
                {RestFeatures, RandState2, maps:put(FeatureID, [FeatureName], Acc)}
        end,
    %% case {DiceNested, DiceUnion, RestFeatures} of
    %%     %% Simple nested schema
    %%     {DiceNested, 1, [_ | _]} when DiceNested > 1 ->
    %%         {LeftFeatures, ReturnedRandState, NestedSchema} = do_build_schema(
    %%             AvgDepth,
    %%             RestFeatures,
    %%             NewRandState,
    %%             #{}
    %%         ),
    %%         {LeftFeatures, ReturnedRandState, maps:put(FeatureID, [FeatureName, NestedSchema], Acc)};
    %%     %% Schema with discriminator (union)
    %%     %% TODO: add discriminator
    %%     {DiceNested, 2, [_ | _]} when DiceNested > 1 ->
    %%         {LeftFeatures, ReturnedRandState, NestedSchema} = do_build_schema(
    %%             AvgDepth,
    %%             RestFeatures,
    %%             NewRandState,
    %%             #{}
    %%         ),
    %%         {LeftFeatures, ReturnedRandState, maps:put(FeatureID, [FeatureName, NestedSchema], Acc)};
    %%     _ ->
    %%         {RestFeatures, NewRandState, maps:put(FeatureID, [FeatureName], Acc)}
    %% end,
    do_build_schema(AvgDepth, NextFeatures, NextRandState, NextAcc).

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
        [?SUCHTHAT(
            L,
            list(alphanum_char()),
            L /= []
        ),
         oneof([$_, $-])],
        erlang:list_to_binary(lists:join(Separator, Parts))
    ).

alphanum_char()->
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
            (_, Acc, _) ->
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
            %%    TODO: only discriminator here
            %% ({nested, NestedSchema}, true, [{Id, Name} | _] = RevPath) ->
            %%     case maps:find(Id, Features) of
            %%         error ->
            %%             case maps:find(Name) of
            %%                 {ok, _Value} ->
            %%                     throw({feature_unused, Name});
            %%                 error ->
            %%                     true
            %%             end;
            %%         {ok, undefined} ->
            %%             throw({no_nested_features, lists:reverse(RevPath)});
            %%         {ok, NotMap} when not is_map(NotMap) ->
            %%             throw({single_feature, lists:reverse(RevPath)});
            %%         {ok, NestedFeatures} ->
            %%             NestedEntity =
            %%                 deep_fetch(Entity, name_path(lists:reverse(RevPath))),
            %%             assert_correct_read(NestedSchema, NestedFeatures, NestedEntity)
            %%     end;
            %%
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
                        {{ok, Map}, {ok, _}} when is_map(Map) ->
                            throw({nested_features, Path});
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
                        {no_traverse, Result}
                end
        end,
        true,
        Schema
    ).

assert_correct_compare(Diff, Paths, Entity1, Entity2) ->
    do_assert_correct_compare(Diff, Paths, Entity1, Entity2, []).

%% TODO: check that all paths are different?
%% do_assert_correct_compare(-1, Paths, Entity1, Entity2, _RevPath) when Paths /= [] -> true;
do_assert_correct_compare(Diff, Paths, Entity1, Entity2, RevPath) ->
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
                        do_assert_correct_compare(NestedDiff, NestedPaths, Entity1, Entity2, NewRevPath);
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
                                        Entity1,
                                        Entity2,
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
              {no_traverse, NewAcc}
      end,
      [],
      Schema
     ).
random_pathspecs(Schema) ->
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
                    case random_pathspecs(NestedSchema) of
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
                            NestedPaths = random_pathspecs(NestedSchema),
                            Element = {set, Id, Name, NestedPaths, NestedSchema},
                            [Element | Acc];
                        2 ->
                            Acc
                    end,
                {no_traverse, NewAcc}
        end,
        [],
        Schema
    ).

random_nonexistent_pathspecs(Schema) ->
    traverse_schema(
        fun
            ({set, _Nested}, Acc, _RevPath) ->
                %% Can't create nonexistent paths for sets
                {no_traverse, Acc};

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
            (value, Acc, [{Id, _Name} | _]) ->
                case rand:uniform(2) of
                    1 ->
                        Name = generate_unique_binary(),
                        [{value, Id, Name} | Acc];
                    2 ->
                        Acc
                end;
            (_, Acc, _) ->
                Acc
        end,
        [],
        Schema
    ).

change_values_by_paths(Paths, Entity) ->
    lists:foldl(
        fun
            ({nested, _Id, Name, NestedPaths, _NestedSchema}, EntityAcc) ->
                maps:update_with(
                    Name,
                    fun(NestedEntity) ->
                        NewNestedEntity = change_values_by_paths(NestedPaths, NestedEntity),
                        %% io:fwrite("~p~n", [{change, NestedEntity, NewNestedEntity, NestedPaths}]),
                        NewNestedEntity
                    end,
                    EntityAcc
                );
            ({set, _Id, Name, NestedPaths, NestedSchema}, EntityAcc) ->
                maps:update_with(
                    Name,
                    fun(EntityList) ->
                        change_entity_set(EntityList, NestedPaths, NestedSchema)
                    end,
                    EntityAcc
                );
            ({value, _Id, Name}, EntityAcc) ->
                NewValue = generate_unique_binary(),
                maps:put(Name, NewValue, EntityAcc)
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
            Length = length(Entities),
            order_by(fun(_Element) -> rand:uniform(Length + 1) end, Entities)
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
        -1 -> [[Name]];
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
    PathSpecs = pathspecs(NestedSchema),
    case maps:get(Id, Diff) of
        -1 -> [[Name]];
        SetDiff ->
            lists:flatmap( %% For each element of set
              fun({Index, NestedDiff}) ->
                      lists:flatmap( %%
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
              maps:to_list(SetDiff))
    end.

traverse_schema(Fun, Acc, Schema) ->
    {ResultAcc, _RevPath} = do_traverse_schema(Fun, Acc, Schema, []),
    ResultAcc.

do_traverse_schema(Fun, Acc, Schema, InitRevPath) ->
    maps:fold(
        fun
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
