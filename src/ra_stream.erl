-module(ra_stream).
-behaviour(ra_machine).

-export([
         init/1,
         apply/4,
         init_aux/1,
         handle_aux/6,
         stream/2,
         append/2,
         ack/2,

         start/2,
         seed/2
         ]).

-record(state, {first = -1 :: stream_index(),
                last = -1 :: stream_index(),
                index = #{} :: #{stream_index() => ra_index()}}).

-include("ra.hrl").

-type stream_index() :: non_neg_integer().

-opaque state() :: #state{}.

-type cmd() :: {append, Event :: term()}.

-export_type([state/0]).

init(_) ->
    #state{}.

-spec apply(map(), cmd(), list(), state()) ->
    {state(), list(), stream_index()}.
apply(#{index := RaftIndex}, {append, _Evt}, Effects,
      #state{last = Last, index = Index0} = State) ->
    Index = maps:put(Last+1, RaftIndex, Index0),
    {State#state{index = Index, last = Last+1},
     [{aux, eval} | Effects], ok}.

%% AUX implementation

-type aux_cmd() :: {stream, StartIndex :: stream_index(),
                    MaxInFlight :: non_neg_integer(), pid()} |
                   {ack, pid(), Index :: stream_index()} |
                   {stop_stream, pid()} |
                   eval.

-record(stream, {read_cursor = -1 :: stream_index(),
                 last_ack = -1 :: stream_index(),
                 max_in_flight = 5 :: stream_index()}).

-type aux_state() :: #{pid() =>  #stream{}}.


init_aux(_) ->
    #{}.

-spec handle_aux(term(), term(), aux_cmd(), aux_state(), Log, term()) ->
    {no_reply, aux_state(), Log} when Log :: term().
handle_aux(_RaMachine, _Type, {stream, Start, Max, Pid},
           Aux0, Log0, MacState) ->
    %% this works as a "skip to" function for exisiting streams. It does ignore
    %% any entries that are currently in flight
    %% read_cursor is the next item to read
    Str0 = #stream{read_cursor = Start,
                   last_ack = Start - 1,
                   max_in_flight = Max},
    {Str, Indexes} = evalstream(Str0, MacState),
    Log = stream_entries(Pid, Indexes, Log0),
    AuxState = maps:put(Pid, Str, Aux0),
    {no_reply, AuxState, Log};
handle_aux(_RaMachine, _Type, {ack, AckIndex, Pid},
           Aux0, Log0, MacState) ->
    case Aux0 of
        #{Pid := #stream{read_cursor = Read} = Str0} ->
            %% update stream with ack value, constrain it not to be larger than
            %% the read index in case the streaming pid has skipped around in
            %% the stream by issuing multiple stream/3 commands.
            Str1 = Str0#stream{last_ack = min(Read - 1, AckIndex)},
            {Str, Indexes} = evalstream(Str1, MacState),
            Log = stream_entries(Pid, Indexes, Log0),
            Aux = maps:put(Pid, Str, Aux0),
            {no_reply, Aux, Log};
        _ ->
            {no_reply, Aux0, Log0}
    end;
handle_aux(_RaMachine, _Type, eval,
           Aux0, Log0, MacState) ->
    {Aux, Log} = maps:fold(fun (Pid, S0, {A0, L0}) ->
                                   {S, Indexes} = evalstream(S0, MacState),
                                   {maps:put(Pid, S, A0),
                                    stream_entries(Pid, Indexes, L0)}
                           end, {#{}, Log0}, Aux0),
    {no_reply, Aux, Log}.


stream_entries(Pid, Indexes, Log0) ->
    lists:foldl(fun({StrIdx, LogIdx}, L0) ->
                        {Entries, L} = ra_log:take(LogIdx, 1, L0),
                        [begin
                             Pid ! {stream_delivery, StrIdx, Data}
                         end || {_, _, {'$usr', _, {append, Data}, _}} <- Entries],
                        L
                end, Log0, Indexes).

%% evaluates if a stream needs furter entries
%% returns a list of raft indexes that should be streamed and the updated
%% stream as if the entries had been sent
evalstream(#stream{read_cursor = Read,
                   last_ack = Ack,
                   max_in_flight = Max} = Stream, #state{})
  when Read - Max - 1 =:= Ack ->
    %% max in flight is reached, no further reads should be done
    {Stream, []};
evalstream(#stream{read_cursor = Read} = Stream, #state{last = Last})
      when Read > Last ->
    %% the read cursor is at the head of the stream
    %% nothing to stream
    {Stream, []};
evalstream(#stream{read_cursor = Read} = Stream,
           #state{first = First} = State)
  when Read < First ->
    %%  read cursor is less than first available stream index!
    %%  update read_cursor and last_ack (if we have in flight entries at this
    %%  point they wont be subject to max in flight limiting anymore) this is
    %%  probably acceptable.
    evalstream(Stream#stream{read_cursor = First, last_ack = First}, State);
evalstream(#stream{read_cursor = Read0,
                   last_ack = Ack,
                   max_in_flight = Max} = Stream,
           #state{last = Last, index = Index}) ->
    %% we can read some entries and increment read cursor
    Read = min(Last, Ack + Max),
    Keys = lists:seq(Read0, Read),
    % ?INFO("evalstream we can read keys ~b ~b ~w", [Read0, Read, Keys]),
    {Stream#stream{read_cursor = Read + 1},
     lists:zip(Keys, maps:values(maps:with(Keys, Index)))}.


%% client api

stream(ServerId, FromIndex) ->
    Pid = self(),
    ra_server_proc:cast_aux_command(ServerId, {stream, FromIndex, 5, Pid}).

append(ServerId, Event) ->
    ra:pipeline_command(ServerId, {append, Event}).

ack(ServerId, StreamIndex) ->
    ra_server_proc:cast_aux_command(ServerId, {ack, StreamIndex, self()}).

start(Name, Nodes) ->
    Members = [{Name, N} || N <- Nodes],
    ra:start_cluster(Name, {module, ?MODULE, #{}}, Members).

%% seed the stream with some numeric entries as strings
seed(ServerId, Num) ->
    seed0(ServerId, 0, Num).

seed0(_ServerId, N, N) -> ok;
seed0(ServerId, Curr, Num) ->
    append(ServerId, erlang:integer_to_list(Curr)),
    seed0(ServerId, Curr + 1, Num).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
