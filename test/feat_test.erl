-module(feat_test).

-include("feat.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PROP(Prop), ?PROP(Prop, [])).
-define(PROP(Prop, Opts), ?assertEqual(true, proper:quickcheck(Prop, Opts))).

-spec test() -> _.

-spec simple_test() -> _.
simple_test() ->
    ok.

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
        ),
        [{to_file, user}]
    ).

-spec compare_test() -> _.
compare_test() ->
    ?PROP(
        ?FORALL(
            _Schema,
            schema(),
            %% Fill schema
            %% Refill random fields and remember them
            %% Check that returned comparison is correct
            true
        )
    ).

-spec list_diff_fields_test() -> _.
list_diff_fields_test() ->
    ?PROP(
        ?FORALL(
            _Schema,
            schema(),
            %% Fill schema
            %% Refill random fields and remember path to them
            %% Check that returned paths are correct
            true
        )
    ).

schema() ->
    schema(3).

schema(AvgDepth) ->
    ?LET(
        [Seed, Features],
        [integer(), features()],
        build_schema(AvgDepth, Features, rand:seed(exsss, Seed))
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
    list({integer(), bin_string()}).

bin_string() ->
    ?LET(String, list(alphanum_char()), erlang:list_to_binary(String)).

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
                Value = erlang:list_to_binary(erlang:ref_to_list(make_ref())),

                NamePath = name_path(lists:reverse(RevPath)),
                case deep_force_put(NamePath, Value, Acc) of
                    {ok, NewAcc} -> NewAcc;
                    {error, map_overwrite} -> Acc
                end;
            %% maps:put(Name, Value, Acc);
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
            %% (_, false, _) ->
            %%     false;
            %%                 %%    TODO: only discriminator here

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
                    {{ok, _}, {error, not_found}} ->
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
