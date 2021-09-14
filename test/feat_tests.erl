-module(feat_tests).

-include_lib("eunit/include/eunit.hrl").

-include("feat.hrl").

-spec test() -> _.

%% Serves as an example of the syntax
-define(SCHEMA, #{
    1 =>
        {<<"1">>,
            {set,
                {union, [<<"meta">>, <<"type">>], #{
                    <<"a">> =>
                        {2, #{
                            21 => <<"21">>,
                            22 => 'reserved'
                        }},
                    %% Same variant structure, same feature name
                    <<"a_other">> =>
                        {2, #{
                            21 => <<"21">>,
                            22 => 'reserved'
                        }},
                    %% Same variant structure, different feature name
                    <<"A">> =>
                        {3, #{
                            21 => <<"21">>,
                            22 => 'reserved'
                        }},
                    %% Nested sets
                    <<"b">> =>
                        {4, #{
                            31 => {<<"31">>, {set, #{311 => <<"311">>}}}
                        }},
                    %% Tests correct list diff minimization
                    <<"c">> =>
                        {5, #{
                            41 =>
                                {<<"41">>, #{
                                    411 => {<<"411">>, {set, #{}}},
                                    412 => <<"412">>
                                }}
                        }},
                    <<"unchanged">> => {42, #{}}
                }}}}
}).

-define(REQUEST, #{
    <<"1">> => [
        #{
            <<"meta">> => #{<<"type">> => <<"a">>},
            <<"21">> => <<"a_21">>,
            <<"unused">> => 42
        },
        #{
            <<"meta">> => #{<<"type">> => <<"a">>},
            <<"21">> => <<"a_21">>,
            <<"unused">> => 42
        },
        #{
            <<"meta">> => #{<<"type">> => <<"a">>},
            <<"21">> => <<"a_21">>,
            <<"unused">> => 42
        },
        #{
            <<"meta">> => #{<<"type">> => <<"b">>},
            <<"31">> => [
                #{<<"311">> => <<"b_311_1">>},
                #{<<"311">> => <<"b_311_2">>}
            ]
        },
        #{
            <<"meta">> => #{<<"type">> => <<"c">>},
            <<"41">> => #{
                <<"411">> => [],
                <<"412">> => <<"c_412">>
            }
        },
        #{<<"meta">> => #{<<"type">> => <<"unchanged">>}}
    ]
}).

-define(OTHER_REQUEST, #{
    <<"1">> => [
        #{
            <<"meta">> => #{<<"type">> => <<"a_other">>},
            <<"21">> => <<"a_21_other">>,
            <<"unused">> => 43
        },
        #{
            <<"meta">> => #{<<"type">> => <<"a">>},
            <<"21">> => <<"a_21_other">>,
            <<"unused">> => 43
        },
        #{
            <<"meta">> => #{<<"type">> => <<"A">>},
            <<"21">> => <<"a_21">>,
            <<"unused">> => 43
        },
        #{
            <<"meta">> => #{<<"type">> => <<"b">>},
            <<"31">> => [
                #{<<"311">> => <<"b_311_1_other">>},
                #{<<"311">> => <<"b_311_2">>}
            ]
        },
        #{
            <<"meta">> => #{<<"type">> => <<"c">>},
            <<"41">> => #{
                <<"411">> => [],
                <<"412">> => <<"c_412_other">>
            }
        },
        #{
            <<"meta">> => #{<<"type">> => <<"unchanged">>}
        }
    ]
}).

-spec simple_featurefull_schema_read_test() -> _.
simple_featurefull_schema_read_test() ->
    ?assertEqual(
        #{
            1 => [
                [0, {2, #{21 => feat:hash(<<"a_21">>)}}],
                [1, {2, #{21 => feat:hash(<<"a_21">>)}}],
                [2, {2, #{21 => feat:hash(<<"a_21">>)}}],
                [4, {5, #{41 => #{411 => [], 412 => feat:hash(<<"c_412">>)}}}],
                [
                    3,
                    {4, #{
                        31 => [
                            [1, #{311 => feat:hash(<<"b_311_2">>)}],
                            [0, #{311 => feat:hash(<<"b_311_1">>)}]
                        ]
                    }}
                ],
                [5, {42, #{}}]
            ]
        },
        feat:read(?SCHEMA, ?REQUEST)
    ).

-spec simple_featurefull_schema_compare_test() -> _.
simple_featurefull_schema_compare_test() ->
    ?assertEqual(
        {false, #{
            1 => #{
                0 => {2, ?difference},
                1 => {2, ?difference},
                2 => -1,
                3 => {4, #{31 => #{0 => ?difference}}},
                4 => {5, #{41 => #{412 => ?difference}}}
            }
        }},
        begin
            Features = feat:read(?SCHEMA, ?REQUEST),
            OtherFeatures = feat:read(?SCHEMA, ?OTHER_REQUEST),

            feat:compare(Features, OtherFeatures)
        end
    ).

-spec simple_featurefull_schema_list_diff_fields_test() -> _.
simple_featurefull_schema_list_diff_fields_test() ->
    ?assertEqual(
        [<<"1.0">>, <<"1.1">>, <<"1.2">>, <<"1.3.b.31.0">>, <<"1.4.c.41.412">>],
        begin
            Features = feat:read(?SCHEMA, ?REQUEST),
            OtherFeatures = feat:read(?SCHEMA, ?OTHER_REQUEST),

            {false, Diff} = feat:compare(Features, OtherFeatures),
            feat:list_diff_fields(?SCHEMA, Diff)
        end
    ).
