-module(ra_file_handle).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([start_link/0]).
-export([open/3, close/2]).
-export([sync/2, write/3, read/3, position/3]).
-export([pwrite/3, pwrite/4, pread/3, pread/4]).
-export([default_handler/2, default_handler/3]).

-define(SERVER, ?MODULE).

-record(state, {monitors}).

open(File, Modes, {Mod, Fun}) ->
    Mod:Fun(open, fun() ->
                          gen_server:cast(?MODULE, {open, self()}),
                          file:open(File, Modes)
                  end).

close(Fd, {Mod, Fun}) ->
    Mod:Fun(close, fun() ->
                           gen_server:cast(?MODULE, {close, self()}),
                           file:close(Fd)
                   end).

sync(Fd, {Mod, Fun}) ->
    Mod:Fun(io_sync, fun() -> file:sync(Fd) end).

write(Fd, Bytes, {Mod, Fun}) ->
    Mod:Fun(io_write, iolist_size(Bytes), fun() -> file:write(Fd, Bytes) end).

read(Fd, Bytes, {Mod, Fun}) ->
    Mod:Fun(io_write, Bytes, fun() -> file:read(Fd, Bytes) end).

position(Fd, Location, {Mod, Fun}) ->
    Mod:Fun(io_seek, fun() -> file:position(Fd, Location) end).

pwrite(Fd, LocBytes, {Mod, Fun}) ->
    Mod:Fun(io_write, fun() -> file:pwrite(Fd, LocBytes) end).

pwrite(Fd, Location, Bytes, {Mod, Fun}) ->
    Mod:Fun(io_write, iolist_size(Bytes), fun() -> file:pwrite(Fd, Location, Bytes) end).

pread(Fd, LocBytes, {Mod, Fun}) ->
    Mod:Fun(io_write, fun() -> file:pread(Fd, LocBytes) end).

pread(Fd, Location, Number, {Mod, Fun}) ->
    Mod:Fun(io_read, Number, fun() -> file:pread(Fd, Location, Number) end).

default_handler(_Tag, Fun) ->
    Fun().

default_handler(_, _, Fun) ->
    Fun().

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{monitors = #{}}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({open, Pid}, #state{monitors = Monitors0} = State) ->
    Monitors = case maps:is_key(Pid, Monitors0) of
                   true ->
                       Monitors0;
                   false ->
                       MRef = erlang:monitor(process, Pid),
                       maps:put(Pid, MRef, Monitors0)
               end,
    ets:update_counter(ra_open_file_metrics, Pid, 1, {Pid, 0}),
    {noreply, State#state{monitors = Monitors}};
handle_cast({close, Pid}, State) ->
    ets:update_counter(ra_open_file_metrics, Pid, -1, {Pid, 0}),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _}, #state{monitors = Monitors0} = State) ->
    case maps:take(Pid, Monitors0) of
        {_MRef, Monitors} ->
            ets:delete(ra_open_file_metrics, Pid),
            {noreply, State#state{monitors = Monitors}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
