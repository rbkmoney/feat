-module(feat_utils).

-include("feat.hrl").

-export([
    traverse_schema/3
]).

-type traverse_node() ::
    value
    | {set, feat:schema()}
    | {nested, feat:schema()}
    | {union, DiscriminatorName :: feat:request_key(), feat:schema()}.
-type reverse_traversal_path() :: [{feat:feature_name(), feat:request_key()}].

-type traversal_fun_result(T) :: T | {no_traverse, T}.
-type traversal_fun(T) :: fun((traverse_node(), T, reverse_traversal_path()) -> traversal_fun_result(T)).

%%====================================================================
%% API functions
%%====================================================================

-spec traverse_schema(traversal_fun(T), InitAcc :: T, feat:schema()) -> T when T :: term().
traverse_schema(Fun, Acc, Schema) ->
    do_traverse_schema(Fun, Acc, Schema, []).

%%====================================================================
%% Internal functions
%%====================================================================

do_traverse_schema(Fun, Acc, Schema, RevPath) ->
    maps:fold(
        fun
            (Id, 'reserved', CurrentAcc) ->
                Fun(reserved, CurrentAcc, [{Id, Id} | RevPath]);
            (Id, [Name, UnionSchema = #{?discriminator := [DiscriminatorName]}], CurrentAcc) ->
                %% TODO: no_traverse here?
                Fun({union, DiscriminatorName, maps:remove(?discriminator, UnionSchema)}, CurrentAcc, [
                    {Id, Name}
                    | RevPath
                ]);
            (Id, [Name, Value], CurrentAcc) ->
                NextRevPath = [{Id, Name} | RevPath],
                Arg =
                    {_, NestedSchema} =
                    case Value of
                        {set, Nested} ->
                            {set, Nested};
                        Nested ->
                            {nested, Nested}
                    end,
                case Fun(Arg, CurrentAcc, NextRevPath) of
                    {no_traverse, ReturnedAcc} ->
                        ReturnedAcc;
                    NewAcc ->
                        do_traverse_schema(Fun, NewAcc, NestedSchema, NextRevPath)
                end;
            (Id, [Name], CurrentAcc) ->
                Fun(value, CurrentAcc, [{Id, Name} | RevPath])
        end,
        Acc,
        Schema
    ).
