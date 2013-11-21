%% @doc File (pass through) filesystem for Erlang.
%% @end

-module (fuserlfilesrv).
-export ([ start_link/1 ]).
% -behaviour (fuserl).
-export ([ code_change/3,
           handle_info/2,
           init/1,
           terminate/2,
%%           access/5,
           create/7,
%%           flush/5,
           forget/5,
           fsync/6,
%%           fsyncdir/6,
           getattr/4,
%%           getlk/6,
%%           getxattr/6,
%%           link/6,
%%           listxattr/5,
           lookup/5,
           mkdir/6,
%%           mknod/7,
           open/5,
%%           opendir/5,
           read/7,
           readdir/7,
           readlink/4,
           release/5,
%%           releasedir/5,
%%           removexattr/5,
           rename/7,
           rmdir/5,
           setattr/7,
%%           setlk/7,
%%           setxattr/7,
%%           statfs/4,
           symlink/6,
           unlink/5,
           write/7 ]).


-include_lib ("kernel/include/file.hrl").
-include_lib ("fuserl/include/fuserl.hrl").

-record(fh_ent,
	{
	  fh,    %% file handle
	  fd,    %% file descriptor (undefined if closed)
	  ino,   %% file inode
	  mode,  %% open mode
	  time   %% last access time
	}).

-record(state, { 
	  root,
	  names,   %% name => inode
	  tref,    %% timer reference
	  max_open_size = 5 :: integer(), %% number of open files
	  max_open_time = 5000,  %% keep open max 5s
	  delay_read = 0,
	  delay_write = 0,
	  fh_open,   %% table of open file handles
	  fh_closed  %% table of closed file handles
	 }).

-define(debug(F,A), io:format("~s:~w: debug "++(F)++"\n",[?MODULE,?LINE|(A)])).
-define(warn(F,A), io:format("~s:~w: warn "++(F)++"\n",[?MODULE,?LINE|(A)])).
-define(trace(F,A), io:format("~s:~w: "++(F)++"\n",[?MODULE,?LINE|(A)])).
%% -define(debug(F,A), ok).
%% -define(warn(F,A), ok).
%% -define(traceF,A), ok).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link(Args) ->
    LinkedIn   = proplists:get_value(linked_in, Args, true),
    MountPoint = proplists:get_value(mount_point, Args),
    MountOpts  = proplists:get_value(mount_opts, Args, ""),
    fuserlsrv:start_link (?MODULE, LinkedIn, MountOpts, MountPoint, Args, []).

%-=====================================================================-
%-                           fuserl callbacks                          -
%-=====================================================================-

init(Args) ->
    ?debug("args = ~p", [Args]),
    RootDir     = proplists:get_value(root_directory, Args),
    DelayWrite  = proplists:get_value(delay_write, Args,0),
    DelayRead   = proplists:get_value(delay_read, Args,0),
    MaxOpenTime = proplists:get_value(max_open_time, Args, 5000),
    MaxOpenSize = proplists:get_value(max_open_size, Args, 5),
    Names    = ets:new(fuse_ino, []),
    FhOpen   = ets:new(fh_open, [{keypos,#fh_ent.fh}]),
    FhClosed = ets:new(fh_closed, [{keypos,#fh_ent.fh}]),
    case read_stat_(RootDir,0) of
	{ok,A} ->
	    State = #state { names=Names,  
			     root=RootDir,
			     delay_read = DelayRead,
			     delay_write = DelayWrite,
			     max_open_time = MaxOpenTime,
			     max_open_size = MaxOpenSize,
			     fh_open = FhOpen, 
			     fh_closed = FhClosed },
	    insert_inode(0, 1, "", State),  %% 1 = root
	    insert_inode(0, A#stat.st_ino, "", State), %% alias for real ino
	    { ok, State };
	false ->
	    ?warn("~s is not a directory", [RootDir]),
	    { stop, enodir}
    end.

code_change(_OldVsn, State, _Extra) -> 
    { ok, State }.

handle_info({timeout,TRef,{close_inactive,Time0}}, State)
  when  TRef =:= State#state.tref ->
    ?debug("timeout", []),
    fh_close_inactive(Time0, State),
    Time1 = now_to_us(os:timestamp()),
    State1 = State#state { tref = undefined },
    OpenSize = ets:info(State#state.fh_open, size),
    ?debug("number of open = ~w", [OpenSize]),
    State2 = case OpenSize of
		 0 -> State1;
		 _ -> fh_start_timer(Time1,State1)
	     end,
    { noreply, State2 };
handle_info(_Msg, State) -> 
    { noreply, State }.

terminate(_Reason, _State) -> ok.

%% #fuse_ctx{ uid = Uid, gid = Gid, pid = Pid }

%% access (_Ctx, _Inode, _Mask, _Cont, _State) ->
%%   erlang:throw (not_implemented).

create(_Ctx, Parent, BinName, Mode, Fi, _Cont, State) ->
    ?debug("create(~p)", [{Parent,BinName,Mode,Fi}]),
    case lookup_path_name(Parent, State) of
	false ->
	    { #fuse_reply_err{ err=enoent }, State };
	ParentName ->
	    Name = binary_to_list(BinName),
	    PathName = if ParentName =:= "" -> Name;
			  true -> filename:join(ParentName, Name)
		       end,
	    RealName = filename:join(State#state.root, PathName),
	    OpenMode = open_mode_(Fi#fuse_file_info.flags),
	    case file:open(RealName, OpenMode) of
		{ok,Fd} ->
		    case write_mode_(RealName, Mode) of
			ok ->
			    case read_stat_(RealName,0) of
				{ok,A} ->
				    %% try refactor this a bit
				    Inode = A#stat.st_ino,
				    {Fh,State1} = fh_create_open(Inode, 
								 OpenMode,
								 Fd, State),
				    insert_inode(Parent,Inode,Name,State),
				    R = #fuse_entry_param { ino = Inode,
							    generation = 1,
							    attr_timeout_ms = 1000,
							    entry_timeout_ms = 1000,
							    attr = A },
				    Fi1 = Fi#fuse_file_info { fh = Fh },
				    { #fuse_reply_create { fuse_entry_param = R,
							   fuse_file_info = Fi1 },
				      State1 };
				{error,Err} ->
				    file:close(Fd),
				    { #fuse_reply_err{ err=Err }, State }
			    end;
			{error,Err} ->
			    file:close(Fd),
			    { #fuse_reply_err{ err=Err }, State }
		    end;
		{error,Err} ->
		    { #fuse_reply_err{ err=Err }, State }
	    end
    end.

%% flush (_Ctx, _Inode, _Fi, _Cont, _State) ->
%%   erlang:throw (not_implemented).

forget(_Ctx, Inode, Nlookup, _Cont, State) ->
    ?debug("forget(~p)", [{Inode,Nlookup}]),
    if Nlookup =:= 0 ->
	    delete_inode(Inode, State);
       true ->
	    %% wait until Nlookup = 0
	    ok
    end,
    { #fuse_reply_none{}, State}.


fsync(_Ctx, Inode, IsDataSync, Fi, _Cont, State) ->
    ?debug("fsync(~p)", [{Inode,IsDataSync,Fi}]),
    case fh_open(Fi#fuse_file_info.fh, State) of
	{ok,{Fd,State1}} ->
	    Res = if IsDataSync -> file:datasync(Fd);
		     true -> file:sync(Fd)
		  end,
	    case Res of
		ok -> 
		    { #fuse_reply_err{ err = ok }, State1};
		{error,Err} -> 
		    { #fuse_reply_err{ err = Err }, State1}
	    end;
	{error,Err} ->
	    { #fuse_reply_err{ err = Err }, State}
    end.

%% fsyncdir (_Ctx, _Inode, _IsDataSync, _Fi, _Cont, _State) ->
%%   erlang:throw (not_implemented).

getattr(_Ctx, Inode, _Cont, State) ->
    ?debug("getattr(~p)", [{Inode}]),
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	RealName ->
	    case read_stat_(RealName,Inode) of
		{ok,A} ->
		    R = #fuse_reply_attr { attr = A,
					   attr_timeout_ms = 1000 },
		    { R, State };
		{error,Err} ->
		    { #fuse_reply_err{ err = Err }, State }
	    end
    end.

%% getlk (_Ctx, _Inode, _Fi, _Lock, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% getxattr (_Ctx, _Inode, _Name, _Size, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% link (_Ctx, _Ino, _NewParent, _NewName, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% listxattr (_Ctx, _Ino, _Size, _Cont, _State) ->
%%   erlang:throw (not_implemented).

lookup(_Ctx, Parent, BinName, _Cont, State) ->
    ?debug("lookup(~p)", [{Parent,BinName}]),
    case lookup_path_name(Parent, State) of
	false -> 
	    { #fuse_reply_err{ err = enoent }, State };
	ParentName ->
	    Name = binary_to_list(BinName),
	    PathName =  if ParentName =:= "" -> Name;
			   true -> filename:join(ParentName,Name)
			end,
	    RealName = filename:join(State#state.root,PathName),
	    case read_stat_(RealName,0) of
		{ok,A} ->
		    insert_inode(Parent,A#stat.st_ino,Name,State),
		    R = #fuse_entry_param { ino = A#stat.st_ino,
					    generation = 1,
					    attr_timeout_ms = 1000,
                                            entry_timeout_ms = 1000,
					    attr = A },
		    { #fuse_reply_entry { fuse_entry_param = R }, State };
		{error,Err} ->
		    { #fuse_reply_err{ err = Err }, State }
	    end
    end.

mkdir(_Ctx, ParentInode, BinName, Mode, _Cont, State) ->
    ?debug("mkdir(~p)", [{ParentInode,BinName,Mode}]),
    case lookup_real_name(ParentInode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	RealDirName ->
	    Name = binary_to_list(BinName),
	    DirName = filename:join(RealDirName,Name),
	    case file:make_dir(DirName) of
		ok ->
		    case write_mode_(DirName, Mode) of
			ok ->
			    case read_stat_(DirName,0) of
				{ok,A} ->
				    insert_inode(ParentInode,
						 A#stat.st_ino,Name,State),
				    R = #fuse_entry_param { ino = A#stat.st_ino,
							    generation = 1,
							    attr_timeout_ms = 1000,
							    entry_timeout_ms = 1000,
							    attr = A },
				    { #fuse_reply_entry { fuse_entry_param = R }, State };
				{error,Err} ->
				    { #fuse_reply_err{ err = Err }, State }
			    end;
			{error,Err} ->
			    { #fuse_reply_err{ err = Err }, State }
		    end;
		{error,Err} ->
		    { #fuse_reply_err{ err = Err }, State }
	    end
    end.

%% mknod(_Ctx, _ParentInode, _Name, _Mode, _Dev, _Cont, _State) ->
%%   erlang:throw (not_implemented).

open(_, Inode, Fi = #fuse_file_info{}, _, State) ->
    ?debug("open(~p)", [{Inode,Fi}]),
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	_RealName ->
	    Mode = open_mode_(Fi#fuse_file_info.flags),
	    Fh = fh_create_closed(Inode, Mode, State),
	    case fh_open(Fh, State) of
		{ok,{_Fd,State1}} ->
		    %% DirectIo = Fi#fuse_file_info.flags band ?O_DIRECT =/= 0,
		    %% KeepCache = ?
		    Fi1 = Fi#fuse_file_info { fh = Fh
					      %% direct_io = DirectIo,
					      %% keep_cache = KeepCache
					    },
		    { #fuse_reply_open{ fuse_file_info = Fi1 }, State1 };
		{error,Err} ->
		    fh_remove(Fh, State),
		    { #fuse_reply_err{ err = Err }, State }
	    end
    end.

%% opendir(_Ctx, _Inode, _Fi, _Cont, _State) ->
%%   erlang:throw (not_implemented).

read(_Ctx, Inode, Size, Offset, Fi, _, State) ->
    ?debug("read(~p)", [{Inode,Size,Offset,Fi}]),
    case fh_open(Fi#fuse_file_info.fh, State) of
	{ok,{Fd,State1}} ->
	    case file:pread(Fd, Offset, Size) of
		{ok,Data} ->
		    timer:sleep(State#state.delay_read),
		    Sz = erlang:iolist_size(Data),
		    { #fuse_reply_buf{ buf = Data, 
				       size = Sz }, State1 };
		{error,Err} ->
		    { #fuse_reply_err{ err = Err }, State1 }
	    end;
	{error, Err} ->
	    { #fuse_reply_err{ err = Err }, State }
    end.

readdir (_, Inode, Size, Offset, _Fi, _, State) ->
    ?debug("readdir(~p)", [{Inode,Size,Offset,_Fi}]),
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	RealDirName ->
	    case file:list_dir(RealDirName) of
		{ok,DirList} ->
		    %% fixme: add . and ..
		    List = dir_list_(Inode,RealDirName,DirList,
				     -Offset,Size,State,[]),
		    { #fuse_reply_direntrylist { direntrylist = List }, State};
		{error, Err} ->
		    { #fuse_reply_err{ err = Err }, State }
	    end
    end.

readlink(_Ctx, Inode, _Cont, State) ->
    ?debug("readlink(~p)", [{Inode}]),
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	RealName ->
	    case file:read_link(RealName) of
		{ok,Link} ->
		    { #fuse_reply_readlink { link = Link }, State};
		{error, Err} ->
		    { #fuse_reply_err{ err = Err }, State }
	    end
    end.

release(_Ctx, _Inode, Fi, _Cont, State) ->
    ?debug("release(~p)", [{_Inode,Fi}]),
    fh_remove(Fi#fuse_file_info.fh, State),
    %% check Fi#fuse_file_info.flush
    { #fuse_reply_err{ err = ok }, State }.

%% releasedir(_Ctx, _Inode, _Fi, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% removexattr(_Ctx, _Inode, _Name, _Cont, _State) ->
%%   erlang:throw (not_implemented).

rename(_Ctx, Parent, BinName, NewParent, BinNewName, _Cont, State) ->
    ?debug("rename(~p)", [{Parent,BinName,NewParent,BinNewName}]),
    case lookup_real_name(Parent, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	ParentName ->
	    Name = binary_to_list(BinName),
	    Source = filename:join(ParentName, Name),
	    SrcNode = read_inode_(Source),
	    case lookup_real_name(NewParent, State) of
		false ->
		    { #fuse_reply_err{ err = enoent }, State };
		NewParentName ->
		    NewName = binary_to_list(BinNewName),
		    Destination = filename:join(NewParentName, NewName),
		    case file:rename(Source, Destination) of
			ok ->
			    DstNode = read_inode_(Destination),
			    ?trace("srcnode=~w, dstnode=~w", [SrcNode,DstNode]),
			    %% Always SrcNode == DstNode?
			    delete_inode(SrcNode, State),
			    insert_inode(NewParent,DstNode,NewName,State),
			    { #fuse_reply_err{ err = ok }, State };
			{error,Err} ->
			    { #fuse_reply_err{ err = Err }, State }
		    end
	    end
    end.


rmdir(_Ctx, Inode, BinName, _Cont, State) ->
    ?debug("rmdir(~p)", [{Inode,BinName}]),
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	ParentName ->
	    Name = binary_to_list(BinName),
	    RealName = filename:join(ParentName, Name),
	    case read_stat_(RealName,0) of
		{ok,A} ->
		    case file:del_dir(RealName) of
			ok ->
			    delete_inode(A#stat.st_ino, State),
			    { #fuse_reply_err{err=ok}, State};
			{error,Err} ->
			    { #fuse_reply_err{err=Err}, State}
		    end;
		{error,Err} ->
		    { #fuse_reply_err{err=Err}, State}
	    end
    end.


setattr(_Ctx, Inode, Attr, ToSet, Fi, _Cont, State) ->
    ?debug("setattr(~p)", [{Inode,Attr,ToSet,Fi}]),
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	RealName ->
	    {Fd1,State1} = 
		if is_record(Fi, fuse_file_info) ->
			case fh_open(Fi#fuse_file_info.fh, State) of
			    {ok,{Fd0,State0}} -> {Fd0,State0};
			    _Error -> {null,State}
			end;
		   true -> { null, State}
		end,
	    case write_stat_(RealName, Attr, ToSet, Fd1) of
		ok ->
		    case read_stat_(RealName,Inode) of
			{ok,A} ->
			    R = #fuse_reply_attr { attr = A,
						   attr_timeout_ms = 1000 },
			    { R, State1 };
			{error,Err} ->
			    { #fuse_reply_err{ err = Err }, State1 }
		    end;
		{error,Err} ->
		    { #fuse_reply_err{ err = Err }, State1 }
	    end
    end.
	    
%% setlk(_Ctx, _Inode, _Fi, _Lock, _Sleep, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% setxattr(_Ctx, _Inode, _Name, _Value, _Flags, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% statfs(_Ctx, Inode, _Cont, State) ->
%%    { #fuse_reply_statfs{ statvfs = StatVFS }, State}.
%%


symlink(_Ctx, BinLink, Inode, BinName, _Cont, State) ->
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	ParentName ->
	    Name = binary_to_list(BinName),
	    Link = binary_to_list(BinLink),
	    RealName = filename:join(ParentName, Name),
	    case file:make_symlink(Link, RealName) of
		ok ->
		    case read_stat_(RealName,0) of
			{ok,A} ->
			    insert_inode(Inode,A#stat.st_ino,Name,State),
			    R = #fuse_entry_param { ino = A#stat.st_ino,
						    generation = 1,
						    attr_timeout_ms = 1000,
						    entry_timeout_ms = 1000,
						    attr = A },
			    {#fuse_reply_entry{ fuse_entry_param = R }, State};
			{error,Err} ->
			    { #fuse_reply_err{ err = Err }, State }
		    end;
		{error,Err} ->
		    { #fuse_reply_err{ err = Err }, State }
	    end
    end.


unlink (_Ctx, Inode, BinName, _Cont, State) ->
    ?debug("unlink(~p)", [{Inode,BinName}]),
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	ParentName ->
	    Name = binary_to_list(BinName),
	    RealName = filename:join(ParentName, Name),
	    case read_stat_(RealName,0) of
		{ok,A} ->
		    case file:delete(RealName) of
			ok ->
			    delete_inode(A#stat.st_ino, State),
			    { #fuse_reply_err{err=ok}, State};
			{error,Err} ->
			    { #fuse_reply_err{err=Err}, State}
		    end;
		{error,Err} ->
		    { #fuse_reply_err{err=Err}, State}
	    end
    end.			

write(_Ctx, _Inode, Data, Offset, Fi, _Cont, State) ->
    ?debug("write(~p)", [{_Inode,Data,Offset,Fi}]),
    case fh_open(Fi#fuse_file_info.fh, State) of
	{ok,{Fd,State1}} ->
	    case file:pwrite(Fd, Offset, Data) of
		ok ->
		    timer:sleep(State#state.delay_write),
		    Sz = erlang:iolist_size(Data),
		    { #fuse_reply_write{ count = Sz }, State1};
		{error,Err} ->
		    { #fuse_reply_err{ err = Err }, State1 }
	    end;
	{error, Err} ->
	    { #fuse_reply_err{ err = Err }, State }
    end.

open_mode_(Flags) ->
    lists:append(
      [[raw,binary],
       case Flags band ?O_ACCMODE of
	   ?O_RDONLY -> [read];
	   ?O_WRONLY -> [write];
	   ?O_RDWR   -> [read,write]
       end,
       if Flags band ?O_APPEND =/= 0 -> [append]; true -> [] end,
       if Flags band ?O_EXCL =/= 0 -> [exclusive]; true -> [] end]).


%% create a new file handle entry return the new File handle
fh_create_closed(Inode, Mode, State) ->
    Time = Fh = now_to_us(),
    E = #fh_ent { fh=Fh, ino=Inode, mode=Mode, time=Time },
    ?debug("fh_insert fh=~w", [Fh]),
    ets:insert(State#state.fh_closed, E),
    Fh.

fh_create_open(Inode, Mode, Fd, State) ->
    Time = Fh = now_to_us(),
    E = #fh_ent { fh = Fh, fd=Fd, ino=Inode, mode=Mode, time=Time },
    State1 = fh_start_timer(Time,State),
    ets:insert(State1#state.fh_open, E),
    {Fh, State1}.

fh_remove(Fh, State) ->
    fh_close(Fh, State),
    ets:delete(State#state.fh_closed, Fh),
    ok.

%% close and open file in the file cache
%% try to terminate cleanup timer?
fh_close(Fh, State) ->
    case ets:lookup(State#state.fh_open, Fh) of
	[] -> ok;
	[E] ->
	    ?debug("fh_close fh=~w, fd=~w", [Fh,E#fh_ent.fd]),
	    file:close(E#fh_ent.fd),
	    ets:delete(State#state.fh_open, Fh),
	    E1 = E#fh_ent { fd = undefined },
	    ets:insert(State#state.fh_closed,E1),
	    ok
    end.

%% return {ok,{Fd,State1}} | {error,Reason}
fh_open(Fh, State) ->
    case ets:lookup(State#state.fh_open, Fh) of
	[E] ->
	    fh_tick(Fh, State),
	    {ok, {E#fh_ent.fd,State}};
	[] ->
	    fh_open_closed(Fh, State)
    end.

fh_open_closed(Fh, State) ->
    case ets:lookup(State#state.fh_closed, Fh) of
	[] ->
	    {error, enoent};
	[E] ->
	    case lookup_real_name(E#fh_ent.ino, State) of
		false ->
		    {error, enoent};
		Filename ->
		    fh_open_name(Filename, E, State)
	    end
    end.

fh_open_name(Filename, E, State) ->
    fh_trim_open(State),
    case file:open(Filename, E#fh_ent.mode) of
	{ok,Fd} ->
	    Fh = E#fh_ent.fh,
	    ?debug("fh_open fh=~w, fd=~w", [Fh,Fd]),
	    Time0 = now_to_us(os:timestamp()),
	    %% start cleaup timer if needed
	    State1 = fh_start_timer(Time0,State),
	    E1 = E#fh_ent { fd = Fd,time = Time0 },
	    ets:delete(State1#state.fh_closed, Fh),
	    ets:insert(State1#state.fh_open, E1),
	    {ok,{Fd,State1}};
	Error -> Error
    end.
    
fh_start_timer(Time0, State) ->
    if State#state.tref =:= undefined ->
	    ?debug("start timer", []),
	    TRef = erlang:start_timer(State#state.max_open_time, self(),
				      {close_inactive, Time0}),
	    State#state { tref = TRef };
       true ->
	    ?debug("timer running", []),
	    State
    end.

fh_trim_open(State) ->
    Sz = ets:info(State#state.fh_open, size),
    fh_trim_open(State#state.max_open_size - Sz, State).

fh_trim_open(I, State) when I =< 0 ->
    fh_trim_oldest(State),
    fh_trim_open(I+1, State);
fh_trim_open(_, _State) ->
    ok.

fh_trim_oldest(State) ->
    TFh =
	ets:foldl(
	  fun(E, Acc) ->
		  [{E#fh_ent.time, E#fh_ent.fh} | Acc]
	  end, [], State#state.fh_open),
    case lists:keysort(1, TFh) of
	[{_Time,Fh}|_] ->
	    ?debug("trim oldest: ~p", [TFh]),
	    fh_close(Fh, State);
	[] ->
	    ok
    end.


fh_close_inactive(Time0, State) ->
    MaxOpenTime = State#state.max_open_time*1000,
    ets:foldl(
      fun(E,_Acc) ->
	      if E#fh_ent.time - Time0 < MaxOpenTime -> ok;
		 true -> fh_close(E#fh_ent.fh, State)
	      end
      end, ok, State#state.fh_open).

fh_tick(Fh, State) ->
    Time = now_to_us(os:timestamp()),
    ets:update_element(State#state.fh_open, Fh, {#fh_ent.time, Time}).


%% read directory entries, first skip offset number of (negative) entries
%% then fill with entries until full.		    
dir_list_(Inode,RDirName,[_|Names],Offs,Size,State,Acc) when Offs < 0 ->
    dir_list_(Inode,RDirName,Names,Offs+1,Size,State,Acc);
dir_list_(Inode,RDirName,[Name|Names],Offs,Size,State,Acc) when Size > 0 ->
    PathName = filename:join(RDirName,Name),
    case read_stat_(PathName,0) of
	{ok,A} ->
	    E = #direntry { name = Name, offset = Offs+1, stat = A },
	    insert_inode(Inode,A#stat.st_ino,Name,State),
	    Size1 = Size - fuserlsrv:dirent_size(E),
	    if Size1 < 0 ->
		    lists:reverse(Acc);
	       true ->
		    dir_list_(Inode,RDirName,Names,Offs+1,Size-1,State,[E|Acc])
	    end;
	{ error, _Err } ->
	    ?warn("unable to read ~s [~w]", [PathName,_Err]),
	    %% skip
	    dir_list_(Inode,RDirName,Names,Offs,Size,State,Acc)
    end;
dir_list_(_Inode,_RDirName,[],_Offs,_Size,_State,Acc) ->
    lists:reverse(Acc);
dir_list_(_Inode,_RDirName,_Names,_Offs,Size,_State,Acc) when Size =:= 0 -> 
    lists:reverse(Acc).

%%
%% We could store {INode, {ParentINode,Name}} for a bit more
%% compact name representation and also a way for finding dir etc.
%%
insert_inode(IParent, INode, Name, State) ->
    ?trace("insert: ~w => (~w,~s)", [INode,IParent,Name]),
    ets:insert(State#state.names, {INode, {IParent,Name}}).

delete_inode(Inode, State) when is_integer(Inode) ->
    ?trace("delete: ~w", [Inode]),
    ets:delete(State#state.names, Inode).

lookup_real_name(Inode, State) when is_integer(Inode) ->
    Res = lookup_real_name_(Inode, State),
    ?trace("lookup_real_name: ~w = ~p", [Inode,Res]),
    Res.
    
lookup_real_name_(INode, State) ->
    case lookup_path_name_(INode, State) of
	false -> false;
	Name -> filename:join(State#state.root, Name)
    end.

lookup_path_name(Inode, State) when is_integer(Inode) ->
    Res = lookup_path_name_(Inode, State),
    ?trace("lookup_path_name: ~w = ~p", [Inode,Res]),
    Res.

%% construct the path name from inode.
lookup_path_name_(1, _State) -> 
    "";
lookup_path_name_(Inode, State) ->
    case ets:lookup(State#state.names, Inode) of
	[] ->
	    ?trace("  lookup: ~w = false", [Inode]),
	    false;
	[{_,{0,Name}}] ->
	    ?trace("  lookup: ~w = ~p", [Inode,Name]),
	    Name;  %% null parent
	[{_,{ParentINode,Name}}] ->
	    ?trace("  lookup: ~w = ~p", [Inode,Name]),
	    case lookup_path_name_(ParentINode,State) of
		false -> false;
		"" -> Name;
		ParentName -> filename:join(ParentName, Name)
	    end
    end.

-define(FUSE_SET_ATTR_MASK, 2#111111). %% FIXME! should be in fuserl?
-define(FUSE_MODE_MASK, 8#7777).

write_mode_(Filename, Mode) ->
    FI = #file_info { mode = Mode band ?FUSE_MODE_MASK },
    file:write_file_info(Filename, FI, [{time,posix}]).

write_stat_(Filename, Stat, Mask, Fd) ->
    FI = set_file_info(Stat, Mask band ?FUSE_SET_ATTR_MASK, #file_info {}),
    if Fd =/= null, is_integer(FI#file_info.size) ->
	    R1 = file:position(Fd, FI#file_info.size),
	    R2 = file:truncate(Fd),
	    ?debug("set_size ~w = ~p,~p", [FI#file_info.size,R1,R2]),
	    R2;
       true ->
	    ok
    end,
    ?debug("write_stat_ ~s = ~p", [Filename, FI]),
    file:write_file_info(Filename, FI, [{time,posix}]).

set_file_info(_Stat, 0, FI) -> FI;
set_file_info(Stat, Mask, FI) when Mask band ?FUSE_SET_ATTR_MODE =/= 0 ->
    FI1 = FI#file_info { mode = Stat#stat.st_mode band ?FUSE_MODE_MASK },
    set_file_info(Stat, Mask band (bnot ?FUSE_SET_ATTR_MODE), FI1);
set_file_info(Stat, Mask, FI) when Mask band ?FUSE_SET_ATTR_UID =/= 0 ->
    FI1 = FI#file_info { uid = Stat#stat.st_uid },
    set_file_info(Stat, Mask band (bnot ?FUSE_SET_ATTR_UID), FI1);
set_file_info(Stat, Mask, FI) when Mask band ?FUSE_SET_ATTR_GID =/= 0 ->
    FI1 = FI#file_info { gid = Stat#stat.st_gid },
    set_file_info(Stat, Mask band (bnot ?FUSE_SET_ATTR_GID), FI1);
set_file_info(Stat, Mask, FI) when Mask band ?FUSE_SET_ATTR_SIZE =/= 0 ->
    FI1 = FI#file_info { size = Stat#stat.st_size }, %% is this supported?
    set_file_info(Stat, Mask band (bnot ?FUSE_SET_ATTR_SIZE), FI1);
set_file_info(Stat, Mask, FI) when Mask band ?FUSE_SET_ATTR_ATIME =/= 0 ->
    FI1 = FI#file_info { atime = Stat#stat.st_atime },
    set_file_info(Stat, Mask band (bnot ?FUSE_SET_ATTR_ATIME), FI1);
set_file_info(Stat, Mask, FI) when Mask band ?FUSE_SET_ATTR_MTIME =/= 0 ->
    FI1 = FI#file_info { mtime = Stat#stat.st_mtime },
    set_file_info(Stat, Mask band (bnot ?FUSE_SET_ATTR_MTIME), FI1).
%% crash

read_inode_(File) ->
    case read_stat_(File, 0) of
	{ok,A} -> A#stat.st_ino;
	_ -> false
    end.
    
%% read file stat
read_stat_(File,UseInode) ->
    case file:read_link_info(File,[{time,posix}]) of
	{ok,FI} ->
	    IFMT =
		case FI#file_info.type of
		    device    ->  ?S_IFCHR; %% erlang only specify device!
		    directory -> ?S_IFDIR; 		
		    other     -> ?S_IFSOCK;  %% or fifo? guess?
		    regular -> ?S_IFREG;
		    symlink -> ?S_IFLNK
		end,
	    Inode = if UseInode =:= 0 -> FI#file_info.inode;
		       true -> UseInode
		    end,
	    {ok, #stat { st_ino   = Inode,
			 st_mode  = IFMT bor FI#file_info.mode,
			 st_nlink = FI#file_info.links,
			 st_uid   = FI#file_info.uid,
			 st_gid   = FI#file_info.gid,
			 st_size  = FI#file_info.size,
			 st_atime = datetime_to_unix(FI#file_info.atime),
			 st_mtime = datetime_to_unix(FI#file_info.mtime),
			 st_ctime = datetime_to_unix(FI#file_info.ctime)
		       }};
	Error ->
	    Error
    end.

%% now based timestamp and node uniq id's
now_to_us() ->
    now_to_us(now()).
now_to_us({M,S,U}) ->
    (M*1000000+S)*1000000 + U.


datetime_to_unix(undefined) ->
    0;
datetime_to_unix(Posix) when is_integer(Posix) ->
    Posix;
datetime_to_unix(DateTime) ->
    Secs = calendar:datetime_to_gregorian_seconds(DateTime),
    Secs - 62167219200.
