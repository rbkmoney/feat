-module(feat_tests).

-include_lib("eunit/include/eunit.hrl").

-include("feat.hrl").

-define(COMMON_VALUES, 1000).
-define(COMMON_VALUE, 1111).
-define(COMMON_VALUE_2, 1112).
-define(UNION, 2000).

%% Serves as an example of the syntax
-define(SCHEMA, #{
    1 =>
        {<<"1">>,
            {set, #{
                ?COMMON_VALUES => #{
                    ?COMMON_VALUE => <<"common_value">>,
                    ?COMMON_VALUE_2 => <<"common_value_2">>
                },
                ?UNION =>
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
                        <<"unchanged">> => {42, #{}},
                        <<"invalid">> => {invalid_spec}
                    }}
            }}}
}).

-define(REQUEST, #{
    <<"1">> => [
        #{
            <<"meta">> => #{<<"type">> => <<"a">>},
            <<"21">> => <<"a_21">>,
            <<"unused">> => 42,
            <<"common_value">> => <<"common">>,
            <<"common_value_2">> => <<"common_2">>
        },
        #{
            <<"meta">> => #{<<"type">> => <<"a">>},
            <<"21">> => <<"a_21">>,
            <<"unused">> => 42,
            <<"common_value">> => <<"common">>
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
            <<"unused">> => 43,
            <<"common_value">> => <<"common">>,
            <<"common_value_2">> => <<"other_common_2">>
        },
        #{
            <<"meta">> => #{<<"type">> => <<"a">>},
            <<"21">> => <<"a_21_other">>,
            <<"unused">> => 43
            %% When comparing two requests, if the old one (the latter argument) has no field
            %% that is present in the new request, it's ignored for idempotency compatibility reasons
            %% <<"common_value">> => <<"no_value">>
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

-spec test() -> _.

-spec simple_featurefull_schema_read_test() -> _.
simple_featurefull_schema_read_test() ->
    ?assertEqual(
        #{
            1 => [
                [
                    0,
                    #{
                        ?COMMON_VALUES => #{
                            ?COMMON_VALUE => feat:hash(<<"common">>),
                            ?COMMON_VALUE_2 => feat:hash(<<"common_2">>)
                        },
                        ?UNION => [2, #{21 => feat:hash(<<"a_21">>)}]
                    }
                ],
                [
                    1,
                    #{
                        ?COMMON_VALUES => #{?COMMON_VALUE => feat:hash(<<"common">>), ?COMMON_VALUE_2 => undefined},
                        ?UNION => [2, #{21 => feat:hash(<<"a_21">>)}]
                    }
                ],
                [
                    2,
                    #{
                        ?COMMON_VALUES => #{?COMMON_VALUE => undefined, ?COMMON_VALUE_2 => undefined},
                        ?UNION => [2, #{21 => feat:hash(<<"a_21">>)}]
                    }
                ],
                [
                    4,
                    #{
                        ?COMMON_VALUES => #{?COMMON_VALUE => undefined, ?COMMON_VALUE_2 => undefined},
                        ?UNION => [5, #{41 => #{411 => [], 412 => feat:hash(<<"c_412">>)}}]
                    }
                ],
                [
                    3,
                    #{
                        ?COMMON_VALUES => #{?COMMON_VALUE => undefined, ?COMMON_VALUE_2 => undefined},
                        ?UNION => [
                            4,
                            #{
                                31 => [
                                    [1, #{311 => feat:hash(<<"b_311_2">>)}],
                                    [0, #{311 => feat:hash(<<"b_311_1">>)}]
                                ]
                            }
                        ]
                    }
                ],
                [
                    5,
                    #{
                        ?COMMON_VALUES => #{?COMMON_VALUE => undefined, ?COMMON_VALUE_2 => undefined},
                        ?UNION => [42, #{}]
                    }
                ]
            ]
        },
        feat:read(?SCHEMA, ?REQUEST)
    ).

-spec simple_featurefull_schema_compare_test() -> _.
simple_featurefull_schema_compare_test() ->
    ?assertEqual(
        {false, #{
            1 => #{
                0 => #{?UNION => [2, ?difference], ?COMMON_VALUES => #{?COMMON_VALUE_2 => ?difference}},
                1 => #{?UNION => [2, ?difference]},
                2 => #{?UNION => ?difference},
                3 => #{?UNION => [4, #{31 => #{0 => ?difference}}]},
                4 => #{?UNION => [5, #{41 => #{412 => ?difference}}]}
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
        %% DISCUSS: first two elements are correct, but intuitively misleading:
        %% Looking at diff (can be found in the previous test),
        %% The first element differs in one of common values, yet the union part is differs completely,
        %% and because there was no accessor on the way, <<"1.0">> is correct, but misleading:
        %% we have both <<"1.0">> and <<"1.0.common_value_2">>
        %% The only way to differentiate union values is to encode them directly to field paths
        %% (e.g. <<"1.0.a">> in this case)
        [
            <<"1.0.common_value_2">>,
            <<"1.0">>,
            <<"1.1">>,
            <<"1.2">>,
            <<"1.3.b.31.0">>,
            <<"1.4.c.41.412">>
        ],
        begin
            Features = feat:read(?SCHEMA, ?REQUEST),
            OtherFeatures = feat:read(?SCHEMA, ?OTHER_REQUEST),

            {false, Diff} = feat:compare(Features, OtherFeatures),
            feat:list_diff_fields(?SCHEMA, Diff)
        end
    ).

-dialyzer({nowarn_function, fail_on_invalid_schema_test/0}).
-spec fail_on_invalid_schema_test() -> _.
fail_on_invalid_schema_test() ->
    ?assertError(
        {invalid_schema, {my, cool, schema}},
        feat:read(#{42 => {my, cool, schema}}, #{<<"key">> => <<"value">>})
    ).

-spec fail_on_missing_variant_test() -> _.
fail_on_missing_variant_test() ->
    #{1 := {_, {set, #{?UNION := UnionSchema}}}} = ?SCHEMA,
    ?assertError(
        {invalid_union_variant_schema, <<"invalid">>, {invalid_spec}, UnionSchema},
        feat:read(?SCHEMA, #{<<"1">> => [#{<<"meta">> => #{<<"type">> => <<"invalid">>}}]})
    ).

-spec all_events_test() -> _.
all_events_test() ->
    Schema = #{
        1 =>
            {<<"key">>,
                {set,
                    {union, [<<"type">>], #{
                        <<"variant">> => {2, #{3 => <<"field">>, 4 => [<<"nested">>, <<"field">>]}},
                        <<"invalid">> => {2, <<"field">>}
                    }}}}
    },
    #{1 := {<<"key">>, {set, UnionSchema}}} = Schema,
    Request = #{
        <<"key">> => [
            #{<<"type">> => <<"variant">>, <<"field">> => <<"value">>},
            #{<<"type">> => <<"variant">>, <<"nested">> => [<<"nope">>]},
            #{<<"type">> => <<"missing">>}
        ]
    },
    Elements = maps:get(<<"key">>, Request),
    [Element0, Element1, Element2] = Elements,
    Pid = self(),
    Handler = fun(Event) ->
        Pid ! Event,
        ok
    end,
    _ = feat:read(Handler, Schema, Request),

    Expected = [
        {request_visited, Request},

        {request_key_visit, <<"key">>, Elements},
        {request_key_visited, <<"key">>, Elements},

        %% NOTE: Elements are out of order due to how set feature encoding is implemented
        {request_index_visit, 2, Element2},
        {request_key_visit, <<"type">>, <<"missing">>},
        {request_key_visited, <<"type">>, <<"missing">>},
        {missing_union_variant, <<"missing">>, Element2, UnionSchema},
        {request_index_visited, 2, Element2},

        {request_index_visit, 0, Element0},
        {request_key_visit, <<"type">>, <<"variant">>},
        {request_key_visited, <<"type">>, <<"variant">>},
        {request_variant_visit, 2, <<"variant">>, Element0},
        {request_key_visit, <<"field">>, <<"value">>},
        {request_key_visited, <<"field">>, <<"value">>},
        {request_variant_visited, 2, <<"variant">>, Element0},
        {request_index_visited, 0, Element0},

        {request_index_visit, 1, Element1},
        {request_key_visit, <<"type">>, <<"variant">>},
        {request_key_visited, <<"type">>, <<"variant">>},
        {request_variant_visit, 2, <<"variant">>, Element1},
        {request_key_visit, <<"nested">>, [<<"nope">>]},
        {invalid_schema_fragment, [<<"field">>], [<<"nope">>]},
        {request_key_visited, <<"nested">>, [<<"nope">>]},
        {request_variant_visited, 2, <<"variant">>, Element1},
        {request_index_visited, 1, Element1}
    ],

    ?assertEqual(Expected, receive_all()).

receive_all() ->
    receive
        Msg -> [Msg | receive_all()]
    after 0 -> []
    end.
