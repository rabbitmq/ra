%% Macros for file write operations that support test-only interception.
%%
%% In production, these expand to direct file:write/2 and file:pwrite/3 calls.
%% In test, an interceptor function stored in a persistent_term can override
%% the behaviour: it receives the operation name and argument list and should
%% return `passthrough' to proceed with the real call, or any other term to
%% use as the result.
%%
%% Setting / clearing the interceptor from test code:
%%   persistent_term:put(?file_interceptor_pt, fun(Op, Args) -> ... end).
%%   persistent_term:erase(?file_interceptor_pt).

%% Defined unconditionally for Dialyzer:
-define(file_interceptor_pt, '$ra_test_file_interceptor').

-ifdef(TEST).

-define(file_write(Fd, Data),
    (fun(__Fd__, __Data__) ->
         try persistent_term:get(?file_interceptor_pt) of
             __Fun__ -> __Fun__(write, [__Fd__, __Data__])
         catch
             error:badarg -> file:write(__Fd__, __Data__)
         end
     end)(Fd, Data)).

-define(file_pwrite(Fd, Loc, Data),
    (fun(__Fd__, __Loc__, __Data__) ->
         try persistent_term:get(?file_interceptor_pt) of
             __Fun__ -> __Fun__(pwrite, [__Fd__, __Loc__, __Data__])
         catch
             error:badarg -> file:pwrite(__Fd__, __Loc__, __Data__)
         end
     end)(Fd, Loc, Data)).

-else.

-define(file_write(Fd, Data), file:write(Fd, Data)).
-define(file_pwrite(Fd, Loc, Data), file:pwrite(Fd, Loc, Data)).

-endif.
