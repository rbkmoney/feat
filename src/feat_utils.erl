-module(feat_utils).

-export([
    zipfold/4
]).

%%====================================================================
%% API functions
%%====================================================================

-spec zipfold(
    fun((K, V1, V2, A) -> A),
    InitAcc :: A,
    #{K => V1},
    #{K => V2}
) -> A.
zipfold(Fun, Acc, M1, M2) ->
    maps:fold(
        fun(Key, V1, AccIn) ->
            case maps:find(Key, M2) of
                {ok, V2} ->
                    Fun(Key, V1, V2, AccIn);
                error ->
                    AccIn
            end
        end,
        Acc,
        M1
    ).
