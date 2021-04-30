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
    {ResultAcc, _RevPath} = do_traverse_schema(Fun, Acc, Schema, []),
    ResultAcc.

%%====================================================================
%% Internal functions
%%====================================================================

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
