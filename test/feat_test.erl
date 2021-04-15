-module(feat_test).

-include("feat.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PROP(Prop), ?PROP(Prop, [])).
-define(PROP(Prop, Opts), ?assert(proper:quickcheck(Prop, [{to_file, user}] ++ Opts))).

-define(RAND_ALG, exsss).
-define(SET_SEED(Seed), _ = rand:seed(?RAND_ALG, Seed)).

-spec test() -> _.

-spec simple_test() -> _.
simple_test() ->
    ok.

%% TODO:
%%

-spec hash_calculatable_test() -> _.
hash_calculatable_test() ->
    ?PROP(?FORALL(Term, term(), is_integer(feat:hash(Term)))).

%% TODO: move all random generation to proper generators
-spec read_test() -> _.
read_test() ->
    ?PROP(
        ?FORALL(
            Schema,
            schema(),
            begin
                Entity = fill_schema(Schema),
                Features = feat:read(Schema, Entity),

                is_map(Features) andalso
                    assert_correct_read(Schema, Features, Entity)
            end
        )
    ).

-spec compare_same_test() -> _.
compare_same_test() ->
    ?PROP(
        ?FORALL(
            [Schema, Seed],
            [schema(), integer()],
            begin
                ?SET_SEED(Seed),

                Entity1 = fill_schema(Schema),
                SomePaths = random_nonexistent_paths(Schema),
                Entity2 = change_values_by_paths(SomePaths, Entity1),

                Features1 = feat:read(Schema, Entity1),
                Features2 = feat:read(Schema, Entity2),

                ?assertEqual(true, feat:compare(Features1, Features2)),
                true
            end
        )
    ).

-spec compare_different_test() -> _.
compare_different_test() ->
    ?PROP(
        ?FORALL(
            [Schema, Seed],
            [schema(), integer()],
            begin
                ?SET_SEED(Seed),

                Entity1 = fill_schema(Schema),

                %% TODO: move to such that
                %% ?assertNotEqual(Entity, Entity2),
                case random_paths(Schema) of
                    [] ->
                        true;
                    SomePaths ->
                        %% erlang:display({paths, Entity1, SomePaths}),
                        Entity2 = change_values_by_paths(SomePaths, Entity1),

                        Features1 = feat:read(Schema, Entity1),
                        Features2 = feat:read(Schema, Entity2),

                        {false, Diff} = feat:compare(Features1, Features2),

                        is_map(Diff) andalso
                            assert_correct_compare(Diff, SomePaths)
                end
            end
        )
    ).

-spec list_diff_fields_test() -> _.
list_diff_fields_test() ->
    ?PROP(
        ?FORALL(
            [Schema, Seed],
            [schema(), integer()],
            begin
                ?SET_SEED(Seed),

                Entity1 = fill_schema(Schema),

                %% TODO: move to such that
                %% ?assertNotEqual(Entity, Entity2),
                case random_paths(Schema) of
                    [] ->
                        true;
                    SomePaths ->
                        Entity2 = change_values_by_paths(SomePaths, Entity1),

                        Features1 = feat:read(Schema, Entity1),
                        Features2 = feat:read(Schema, Entity2),

                        {false, Diff} = feat:compare(Features1, Features2),

                        DiffFields = feat:list_diff_fields(Schema, Diff),
                        ChangedFields = lists:map(
                            fun(Path) ->
                                list_to_binary(
                                    lists:join(
                                        $.,
                                        name_path(Path)
                                    )
                                )
                            end,
                            SomePaths
                        ),

                        is_map(Diff) andalso
                            assertEqualSets(ChangedFields, DiffFields)
                end
            end
        )
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
    {Int, NewRandState} = rand:uniform_s(AvgDepth + 1, RandState),
    {NextFeatures, NextRandState, NextAcc} =
        case {Int, RestFeatures} of
            {Dice, [_ | _]} when Dice > 1 ->
                %% TODO: add discriminator
                %% TODO: add sets
                {LeftFeatures, ReturnedRandState, NestedSchema} = do_build_schema(
                    AvgDepth,
                    RestFeatures,
                    NewRandState,
                    #{}
                ),
                {LeftFeatures, ReturnedRandState, maps:put(FeatureID, [FeatureName, NestedSchema], Acc)};
            _ ->
                {RestFeatures, NewRandState, maps:put(FeatureID, [FeatureName], Acc)}
        end,
    do_build_schema(AvgDepth, NextFeatures, NextRandState, NextAcc).

features() ->
    list({non_neg_integer(), bin_string()}).

bin_string() ->
    ?LET(
        String,
        %% TODO: Compare tests fail without SUCHTHAT... Reason?
        ?SUCHTHAT(
            L,
            list(alphanum_char()),
            L /= []
        ),
        erlang:list_to_binary(String)
    ).

alphanum_char() ->
    oneof([
        integer(16#30, 16#39),
        integer(16#41, 16#5A),
        integer(16#61, 16#7A),
        $_,
        $-
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
                %% TODO: applicable term()s generation
                %% %% TODO?: sparsity (how many are not defined)
                Value = generate_unique_binary(),

                NamePath = name_path(lists:reverse(RevPath)),
                case deep_force_put(NamePath, Value, Acc) of
                    {ok, NewAcc} -> NewAcc;
                    {error, map_overwrite} -> Acc
                end;
            (_, Acc, _) ->
                Acc
        end,
        #{},
        Schema
    ).

assert_correct_read(Schema, Features, Entity) ->
    %% erlang:display({schema, Schema}),
    %% erlang:display({features, Features}),
    %% erlang:display({entity, Entity}),
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
            (value, true, RevPath) ->
                Path = lists:reverse(RevPath),
                %% erlang:display({path, Path}),
                FeatureResult = deep_fetch(id_path(Path), Features),
                EntityResult = deep_fetch(name_path(Path), Entity),
                case {FeatureResult, EntityResult} of
                    {{error, not_found, _}, {error, not_found, _}} ->
                        true;
                    {{error, not_found, _}, {ok, _}} ->
                        throw({feature_unused, Path, Entity, Features, Schema});
                    {{ok, _}, {error, not_found, _}} ->
                        throw({unknown_feature, Path, Entity, Features, Schema});
                    {{ok, Map}, {ok, _}} when is_map(Map) ->
                        throw({nested_features, Path});
                    {{ok, FeatureHash}, {ok, Value}} ->
                        ?assertEqual(FeatureHash, feat:hash(Value)),
                        true;
                    {{error, not_map, ErrorPath}, _} ->
                        throw({feature_not_map, ErrorPath, Path, Entity, Features, Schema});
                    {_, {error, not_map, ErrorPath}} ->
                        throw({entity_not_map, ErrorPath, Path, Entity, Features, Schema})
                end;
            (_, Acc, _) ->
                Acc
        end,
        true,
        Schema
    ).

assert_correct_compare(Diff, Paths) ->
    IdPaths = lists:map(fun id_path/1, Paths),
    %% erlang:display({compare, Diff,
    %%                 IdPaths,
    %%                 do_assert_correct_compare(Diff, IdPaths, [])}),
    [] == do_assert_correct_compare(Diff, IdPaths, []).

do_assert_correct_compare(Diff, Paths, RevPath) ->
    maps:fold(
        fun
            (Key, NestedDiff, PathsAcc) when is_map(NestedDiff) ->
                do_assert_correct_compare(NestedDiff, PathsAcc, [Key | RevPath]);
            (Key, ?difference, PathsAcc) ->
                Path = lists:reverse([Key | RevPath]),
                %% erlang:display({eliminating, PathsAcc, Path}),
                PathsAcc -- [Path]
        end,
        Paths,
        Diff
    ).

random_paths(Schema) ->
    traverse_schema(
        fun
            (value, Acc, RevPath) ->
                case rand:uniform(2) of
                    1 ->
                        [lists:reverse(RevPath) | Acc];
                    2 ->
                        Acc
                end;
            (_, Acc, _) ->
                Acc
        end,
        [],
        Schema
    ).

random_nonexistent_paths(Schema) ->
    traverse_schema(
        fun
            (value, Acc, RevPath) ->
                case rand:uniform(2) of
                    1 ->
                        [{Id, _Name} | Rest] = RevPath,
                        NewRevPath = [{Id, generate_unique_binary()} | Rest],
                        [lists:reverse(NewRevPath) | Acc];
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
        fun(Path, EntityAcc) ->
            NamePath = name_path(Path),
            NewValue = generate_unique_binary(),
            case deep_force_put(NamePath, NewValue, EntityAcc) of
                {ok, NewAcc} -> NewAcc
                %% {error, map_overwrite} ->
                %% erlang:display({map_overwrite, NamePath, EntityAcc, Paths}),
                %% throw(no)
            end
        end,
        Entity,
        Paths
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

traverse_schema(Fun, Acc, Schema) ->
    {ResultAcc, _RevPath} = do_traverse_schema(Fun, Acc, Schema, []),
    ResultAcc.

do_traverse_schema(Fun, Acc, Schema, InitRevPath) ->
    maps:fold(
        fun
            (Id, [Name, NestedSchema], {CurrentAcc, RevPath}) ->
                NewRevPath = [{Id, Name} | RevPath],
                NewAcc = Fun({nested, NestedSchema}, CurrentAcc, NewRevPath),
                {NextAcc, _} = do_traverse_schema(Fun, NewAcc, NestedSchema, NewRevPath),
                {NextAcc, RevPath};
            (Id, [Name], {CurrentAcc, RevPath}) ->
                NewAcc = Fun(value, CurrentAcc, [{Id, Name} | RevPath]),
                {NewAcc, RevPath}
        end,
        {Acc, InitRevPath},
        Schema
    ).
