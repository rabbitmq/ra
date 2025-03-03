-module(ra_seq).

%% open type
-type state() :: [ra:index() | ra:range()].

-export_type([state/0]).



