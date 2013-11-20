-module (fuserlfilesup).
-behaviour (supervisor).

-export ([ start_link/1, init/1 ]).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link(Args) ->
    supervisor:start_link (?MODULE, [Args]).

%-=====================================================================-
%-                         supervisor callbacks                        -
%-=====================================================================-

%% @hidden

init([Args]) ->
  { ok,
    { { one_for_one, 3, 10 },
      [
        { fuserlfilesrv,
          { fuserlfilesrv, start_link, [ Args ] },
          permanent,
          10000,
          worker,
          [ fuserlfilesrv ]
        }
      ]
    }
  }.
