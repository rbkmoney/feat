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
           %% Fill schema
           %% Check that read is correct
           %% (schema_width(Schema) < 5) or (schema_depth(Schema) < 5)
           is_map(Schema)
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
        )).

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
        )).

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
    {Int, NewRandState} = rand:uniform_s(AvgDepth+1, RandState),
    {NextFeatures, NextRandState, NextAcc} =
        case {Int, RestFeatures} of
            {Dice, [_ | _]} when Dice > 1 ->
                %% TODO: add discriminator
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

%% schema_depth(Map) ->
%%     do_schema_depth(Map, 1).

%% do_schema_depth(Map, Depth) ->
%%     traverse_schema(
%%         fun(NestedSchema, MaxDepth) ->
%%             max(
%%                 MaxDepth,
%%                 do_schema_depth(NestedSchema, Depth + 1)
%%             )
%%         end,
%%         Depth,
%%         Map
%%     ).

%% schema_width(Map) ->
%%     do_schema_width(Map, map_size(Map)).
%% do_schema_width(Map, Width) ->
%%     traverse_schema(fun(NestedSchema, MaxWidth) -> max(MaxWidth, schema_width(NestedSchema)) end, Width, Map).

%% traverse_schema(Fun, Acc, Schema) ->
%%     maps:fold(
%%         fun
%%             (_, [_, NestedSchema], CurrentAcc) ->
%%                 Fun(NestedSchema, CurrentAcc);
%%             (_, _, CurrentAcc) ->
%%                 CurrentAcc
%%         end,
%%         Acc,
%%         Schema
%%     ).
