-module (fuserlfilesup).
-behaviour (supervisor).

-export ([ start_link/3, start_link/4, init/1 ]).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link (LinkedIn, RootDir, MountPoint) ->
  start_link (LinkedIn, RootDir, MountPoint, "").

start_link (LinkedIn, RootDir, MountPoint, MountOpts) ->
  supervisor:start_link (?MODULE, [ LinkedIn, RootDir, MountPoint, MountOpts ]).


%-=====================================================================-
%-                         supervisor callbacks                        -
%-=====================================================================-

%% @hidden

init ([ LinkedIn, RootDir, MountPoint, MountOpts ]) ->
  { ok,
    { { one_for_one, 3, 10 },
      [
        { fuserlfilesrv,
          { fuserlfilesrv, start_link,
	    [ LinkedIn, RootDir, MountPoint, MountOpts ] },
          permanent,
          10000,
          worker,
          [ fuserlfilesrv ]
        }
      ]
    }
  }.
