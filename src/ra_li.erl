%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_li).

%% Leaky integrator for rate estimation.
%% Accumulates values over time while exponentially decaying toward zero.
%% Useful for capturing smoothed rates (e.g., messages/second, bytes/second).

-export([
         new/1,
         update/2,
         update/3,
         read/1,
         read/2,
         rate/1,
         rate/2,
         reset/1
        ]).

-record(?MODULE, {decay_time_ms :: pos_integer(),
                  value = 0.0 :: float(),
                  last_update :: integer() | undefined}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
             ]).

%% @doc Create a new leaky integrator with the specified decay time in milliseconds.
-spec new(DecayTimeMs :: pos_integer()) -> state().
new(DecayTimeMs) when is_integer(DecayTimeMs), DecayTimeMs > 0 ->
    #?MODULE{decay_time_ms = DecayTimeMs}.

%% @doc Update the integrator with a value, using current monotonic time.
-spec update(Amount :: number(), state()) -> state().
update(Amount, State) ->
    update(Amount, ts(), State).

%% @doc Update the integrator with a value at the specified timestamp.
-spec update(Amount :: number(), Ts :: integer(), state()) -> state().
update(Amount, Ts, #?MODULE{last_update = undefined} = State) ->
    State#?MODULE{value = float(Amount),
                  last_update = Ts};
update(Amount, Ts, #?MODULE{decay_time_ms = DecayTimeMs,
                            value = Value,
                            last_update = LastUpdate} = State)
  when Ts >= LastUpdate ->
    Elapsed = Ts - LastUpdate,
    Decayed = decay(Value, Elapsed, DecayTimeMs),
    State#?MODULE{value = Decayed + Amount,
                  last_update = Ts};
update(Amount, _Ts, #?MODULE{value = Value} = State) ->
    %% Timestamp went backwards - add amount but don't update last_update
    State#?MODULE{value = Value + Amount}.

%% @doc Read the current decayed value using current monotonic time.
-spec read(state()) -> float().
read(State) ->
    read(ts(), State).

%% @doc Read the current decayed value at the specified timestamp.
-spec read(Ts :: integer(), state()) -> float().
read(_Ts, #?MODULE{last_update = undefined}) ->
    0.0;
read(Ts, #?MODULE{decay_time_ms = DecayTimeMs,
                  value = Value,
                  last_update = LastUpdate}) ->
    Elapsed = max(0, Ts - LastUpdate),
    decay(Value, Elapsed, DecayTimeMs).

%% @doc Return the current rate (value / decay_time_seconds) using current monotonic time.
-spec rate(state()) -> float().
rate(State) ->
    rate(ts(), State).

%% @doc Return the current rate at the specified timestamp.
%% Rate is calculated as value / decay_time, giving units per decay period.
%% For a 1000ms decay time, this gives units per second.
-spec rate(Ts :: integer(), state()) -> float().
rate(Ts, #?MODULE{decay_time_ms = DecayTimeMs} = State) ->
    Value = read(Ts, State),
    %% Convert to rate per second
    Value / DecayTimeMs * 1000.

%% @doc Reset the integrator to zero.
-spec reset(state()) -> state().
reset(#?MODULE{} = State) ->
    State#?MODULE{value = 0.0,
                  last_update = undefined}.

%% Internal functions

ts() ->
    erlang:monotonic_time(millisecond).

decay(Value, Elapsed, _DecayTimeMs) when Elapsed =< 0 ->
    Value;
decay(Value, Elapsed, DecayTimeMs) ->
    Value * math:exp(-Elapsed / DecayTimeMs).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

new_test() ->
    S = new(1000),
    ?assertEqual(1000, S#?MODULE.decay_time_ms),
    ?assertEqual(0.0, S#?MODULE.value),
    ?assertEqual(undefined, S#?MODULE.last_update),
    ok.

update_first_test() ->
    S0 = new(1000),
    S1 = update(100, 0, S0),
    ?assertEqual(100.0, S1#?MODULE.value),
    ?assertEqual(0, S1#?MODULE.last_update),
    ok.

update_with_decay_test() ->
    S0 = new(1000),
    S1 = update(100, 0, S0),
    %% After one decay time, value should be ~36.8% (1/e)
    S2 = update(0, 1000, S1),
    ?assert(abs(S2#?MODULE.value - 100 * math:exp(-1)) < 0.001),
    ok.

update_accumulates_test() ->
    S0 = new(1000),
    S1 = update(100, 0, S0),
    S2 = update(100, 0, S1), %% same timestamp, no decay
    ?assertEqual(200.0, S2#?MODULE.value),
    ok.

read_empty_test() ->
    S0 = new(1000),
    V = read(0, S0),
    ?assertEqual(0.0, V),
    ok.

read_with_decay_test() ->
    S0 = new(1000),
    S1 = update(100, 0, S0),
    %% After 500ms (half decay time), value should be ~60.6%
    V = read(500, S1),
    Expected = 100 * math:exp(-0.5),
    ?assert(abs(V - Expected) < 0.001),
    %% State should be unchanged (read is non-mutating)
    ?assertEqual(0, S1#?MODULE.last_update),
    ok.

rate_test() ->
    S0 = new(1000),
    S1 = update(1000, 0, S0),
    %% With 1000 value and 1000ms decay time, rate should be 1000/s
    R = rate(0, S1),
    ?assertEqual(1000.0, R),
    ok.

rate_with_decay_test() ->
    S0 = new(1000),
    S1 = update(1000, 0, S0),
    %% After one decay time
    R = rate(1000, S1),
    Expected = 1000 * math:exp(-1),
    ?assert(abs(R - Expected) < 0.001),
    ok.

reset_test() ->
    S0 = new(1000),
    S1 = update(100, 0, S0),
    S2 = reset(S1),
    ?assertEqual(0.0, S2#?MODULE.value),
    ?assertEqual(undefined, S2#?MODULE.last_update),
    ?assertEqual(1000, S2#?MODULE.decay_time_ms),
    ok.

negative_elapsed_test() ->
    %% If timestamp goes backwards - add amount but keep last_update unchanged
    S0 = new(1000),
    S1 = update(100, 1000, S0),
    S2 = update(50, 500, S1), %% earlier timestamp
    %% Amount should be added, but last_update should stay at 1000
    ?assertEqual(150.0, S2#?MODULE.value),
    ?assertEqual(1000, S2#?MODULE.last_update),
    %% Subsequent update should decay from the original last_update
    S3 = update(0, 2000, S2),
    Expected = 150.0 * math:exp(-1), %% 1000ms elapsed from last_update=1000
    ?assert(abs(S3#?MODULE.value - Expected) < 0.001),
    ok.

-endif.
