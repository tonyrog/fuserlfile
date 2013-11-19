-module (fuserlfile).
-behaviour (application).
-export ([ start/0,
           start/2,
           stop/0,
           stop/1 ]).

%-=====================================================================-
%-                        application callbacks                        -
%-=====================================================================-

%% @hidden

start () ->
    application:start (fuserl),
    application:start (fuserlfile).

%% @hidden

start (_Type, _Args) ->
    io:format("fuserlfile: start called\n", []),
    { ok, LinkedIn }   = application:get_env (fuserlfile, linked_in),
    { ok, MountPoint } = application:get_env (fuserlfile, mount_point),
    { ok, RootDir}     = application:get_env (fuserlfile, root_directory),
    { ok, MountOpts }  = application:get_env (fuserlfile, mount_opts),
    io:format("fuserlfile: ~p\n", [{LinkedIn,MountPoint,
				    RootDir,MountOpts}]),
    case application:get_env (fuserlfile, make_mount_point) of
	{ ok, false } -> 
	    ok;
	_ ->
	    io:format("make_dir: ~p\n", [MountPoint]),
	    case file:make_dir (MountPoint) of
		ok -> ok;
		{ error, eexist } -> ok
	    end
    end,
    fuserlfilesup:start_link (LinkedIn, RootDir, MountPoint, MountOpts).

%% @hidden

stop () ->
  application:stop (fuserlfile).

%% @hidden

stop (_State) ->
  ok.
