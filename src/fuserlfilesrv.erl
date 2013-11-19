%% @doc File (pass through) filesystem for Erlang.
%% @end

-module (fuserlfilesrv).
-export ([ start_link/3, start_link/4 ]).
% -behaviour (fuserl).
-export ([ code_change/3,
           handle_info/2,
           init/1,
           terminate/2,
%%           access/5,
           create/7,
%%           flush/5,
%%           forget/5,
%%           fsync/6,
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
%%           readlink/4,
%%           release/5,
%%           releasedir/5,
%%           removexattr/5,
%%           rename/7,
           rmdir/5,
           setattr/7,
%%           setlk/7,
%%           setxattr/7,
%%           statfs/4,
%%           symlink/6,
%%           unlink/5,
           write/7 ]).


-include_lib ("kernel/include/file.hrl").
-include_lib ("fuserl/include/fuserl.hrl").

-record (state, { 
	   root,
	   names    %% name => inode
	  }).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link(LinkedIn, RootDir, Dir) ->
  start_link (LinkedIn, RootDir, Dir, "").

start_link(LinkedIn, RootDir, Dir, MountOpts) ->
    fuserlsrv:start_link (?MODULE, LinkedIn, MountOpts, Dir, [RootDir], []).

%-=====================================================================-
%-                           fuserl callbacks                          -
%-=====================================================================-

init([RootDir]) ->
    io:format("root directory = ~p\n", [RootDir]),
    Tab = ets:new(fuse_names, []),
    case read_stat_(RootDir) of
	{ok,A} ->
	    State = #state { names=Tab, root=RootDir },
	    insert_inode(0, 1, "", State),  %% 1 = root
	    insert_inode(0, A#stat.st_ino, "", State), %% alias for real ino
	    { ok, State };
	false ->
	    io:format("fuserlfilesrv: ~s is not a directory\n",
		      [RootDir]),
	    { stop, enodir}
    end.

code_change(_OldVsn, State, _Extra) -> { ok, State }.
handle_info(_Msg, State) -> { noreply, State }.
terminate(_Reason, _State) -> ok.

%% #fuse_ctx{ uid = Uid, gid = Gid, pid = Pid }

%% access (_Ctx, _Inode, _Mask, _Cont, _State) ->
%%   erlang:throw (not_implemented).

create(_Ctx, Parent, Name, Mode, Fi, _Cont, State) ->
    io:format("create(~p)\n", [{Parent,Name,Mode,Fi}]),
    case lookup_path_name(Parent, State) of
	false ->
	    { #fuse_reply_err{ err=enoent }, State };
	ParentName ->
	    PathName = filename:join(ParentName, Name),
	    RealName = filename:join(State#state.root, PathName),
	    case file:open(RealName, [write,binary]) of
		{ok,Fd} -> %% cache?
		    file:close(Fd),
		    case write_mode_(RealName, Mode) of
			ok ->
			    case read_stat_(RealName) of
				{ok,A} ->
				    insert_inode(Parent,A#stat.st_ino,Name,State),
				    R = #fuse_entry_param { ino = A#stat.st_ino,
							    generation = 1,
							    attr_timeout_ms = 1000,
							    entry_timeout_ms = 1000,
							    attr = A },
				    %% maybe update Fi?
				    { #fuse_reply_create { fuse_entry_param = R,
							   fuse_file_info = Fi },
				      State };
				{error,Err} ->
				    { #fuse_reply_err{ err=Err }, State }
			    end;
			{error,Err} ->
			    { #fuse_reply_err{ err=Err }, State }
		    end;
		{error,Err} ->
		    { #fuse_reply_err{ err=Err }, State }
	    end
    end.

%% flush (_Ctx, _Inode, _Fi, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% forget (_Ctx, _Inode, _Nlookup, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% fsync (_Ctx, _Inode, _IsDataSync, _Fi, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% fsyncdir (_Ctx, _Inode, _IsDataSync, _Fi, _Cont, _State) ->
%%   erlang:throw (not_implemented).

getattr(_Ctx, Inode, _Cont, State) ->
    io:format("getattr(~p)\n", [{Inode}]),
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	RealName ->
	    case read_stat_(RealName) of
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
    io:format("lookup(~p)\n", [{Parent,BinName}]),
    case lookup_path_name(Parent, State) of
	false -> 
	    { #fuse_reply_err{ err = enoent }, State };
	ParentName ->
	    Name = binary_to_list(BinName),
	    PathName =  if ParentName =:= "" -> Name;
			   true -> filename:join(ParentName,Name)
			end,
	    RealName = filename:join(State#state.root,PathName),
	    case read_stat_(RealName) of
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
    io:format("mkdir(~p)\n", [{ParentInode,BinName,Mode}]),
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
			    case read_stat_(DirName) of
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
    io:format("open(~p)\n", [{Inode,Fi}]),
    case lookup_path_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	_Name ->
	    %% check FI#fuse_file_info.flags open the file
	    %% cache the file handle => Fd in State
	    { #fuse_reply_open{ fuse_file_info = Fi }, State }
    end.

%% opendir(_Ctx, _Inode, _Fi, _Cont, _State) ->
%%   erlang:throw (not_implemented).

			
read(_Ctx, Inode, Size, Offset, _Fi, _, State) ->
    io:format("read(~p)\n", [{Inode,Size,Offset,_Fi}]),
    case lookup_real_name(Inode, State) of
	false -> 
	    { #fuse_reply_err{ err = enoent }, State };
	RealName ->
	    %% FIXME: cache a cuple of openfiles ... for some time
	    case file:open(RealName, [raw,binary,read]) of
		{ok,Fd} ->
		    case file:pread(Fd, Offset, Size) of
			{ok,Data} ->
			    file:close(Fd),
			    Sz = erlang:iolist_size(Data),
			    { #fuse_reply_buf{ buf = Data, 
					       size = Sz }, State };
			{error,Err} ->
			    file:close(Fd),
			    { #fuse_reply_err{ err = Err }, State }
		    end;
		{error, Err} ->
		    { #fuse_reply_err{ err = Err }, State }
	    end
    end.

readdir (_, Inode, Size, Offset, _Fi, _, State) ->
    io:format("readdir(~p)\n", [{Inode,Size,Offset,_Fi}]),
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

%% readlink(_Ctx, _Inode, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% release(_Ctx, _Inode, _Fi, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% releasedir(_Ctx, _Inode, _Fi, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% removexattr(_Ctx, _Inode, _Name, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% rename(_Ctx, _Parent, _Name, _NewParent, _NewName, _Cont, _State) ->
%%   erlang:throw (not_implemented).

rmdir(_Ctx, Inode, BinName, _Cont, State) ->
    io:format("rmdir(~p)\n", [{Inode,BinName}]),
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	ParentName ->
	    Name = binary_to_list(BinName),
	    RealName = filename:join(ParentName, Name),
	    case read_stat_(RealName) of
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
    io:format("setattr(~p)\n", [{Inode,Attr,ToSet,Fi}]),
    case lookup_real_name(Inode, State) of
	false ->
	    { #fuse_reply_err{ err = enoent }, State };
	RealName ->
	    case write_stat_(RealName, Attr, ToSet) of
		ok ->
		    case read_stat_(RealName) of
			{ok,A} ->
			    R = #fuse_reply_attr { attr = A,
						   attr_timeout_ms = 1000 },
			    { R, State };
			{error,Err} ->
			    { #fuse_reply_err{ err = Err }, State }
		    end;
		{error,Err} ->
		    { #fuse_reply_err{ err = Err }, State }
	    end
    end.
	    
%%   erlang:throw (not_implemented).

%% setlk(_Ctx, _Inode, _Fi, _Lock, _Sleep, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% setxattr(_Ctx, _Inode, _Name, _Value, _Flags, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% statfs(_Ctx, _Inode, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% symlink(_Ctx, _Link, _Inode, _Name, _Cont, _State) ->
%%   erlang:throw (not_implemented).

%% unlink (_Ctx, _Inode, _Name, _Cont, _State) ->
%%  erlang:throw (not_implemented).

write(_Ctx, Inode, Data, Offset, _Fi, _Cont, State) ->
    io:format("write(~p)\n", [{Inode,Data,Offset,_Fi}]),
    case lookup_real_name(Inode, State) of
	false -> 
	    { #fuse_reply_err{ err = enoent }, State };
	RealName ->
	    %% FIXME: cache a cuple of openfiles ... for some time
	    case file:open(RealName, [raw,binary,read,write]) of
		{ok,Fd} ->
		    case file:pwrite(Fd, Offset, Data) of
			ok ->
			    file:close(Fd),
			    Sz = erlang:iolist_size(Data),
			    { #fuse_reply_write{ count = Sz }, State};
			{error,Err} ->
			    file:close(Fd),
			    { #fuse_reply_err{ err = Err }, State }
		    end;
		{error, Err} ->
		    { #fuse_reply_err{ err = Err }, State }
	    end
    end.

%% read directory entries, first skip offset number of (negative) entries
%% then fill with entries until full.		    
dir_list_(Inode,RDirName,[_|Names],Offs,Size,State,Acc) when Offs < 0 ->
    dir_list_(Inode,RDirName,Names,Offs+1,Size,State,Acc);
dir_list_(Inode,RDirName,[Name|Names],Offs,Size,State,Acc) when Size > 0 ->
    PathName = filename:join(RDirName,Name),
    case read_stat_(PathName) of
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
	    io:format("error: unable to read ~s [~w]\n", [PathName,_Err]),
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
    io:format("insert: ~w => (~w,~s)\n", [INode,IParent,Name]),
    ets:insert(State#state.names, {INode, {IParent,Name}}).

delete_inode(INode, State) ->
    ets:delete(State#state.names, INode).

lookup_real_name(Inode, State) ->
    Res = lookup_real_name_(Inode, State),
    io:format("lookup_real_name: ~w = ~p\n", [Inode,Res]),
    Res.
    
lookup_real_name_(INode, State) ->
    case lookup_path_name_(INode, State) of
	false -> false;
	Name -> filename:join(State#state.root, Name)
    end.

lookup_path_name(Inode, State) ->
    Res = lookup_path_name_(Inode, State),
    io:format("lookup_path_name: ~w = ~p\n", [Inode,Res]),
    Res.

%% construct the path name from inode.
lookup_path_name_(1, _State) -> 
    "";
lookup_path_name_(Inode, State) ->
    case ets:lookup(State#state.names, Inode) of
	[] ->
	    io:format("  lookup: ~w = false\n", [Inode]),
	    false;
	[{_,{0,Name}}] ->
	    io:format("  lookup: ~w = ~p\n", [Inode,Name]),
	    Name;  %% null parent
	[{_,{ParentINode,Name}}] ->
	    io:format("  lookup: ~w = ~p\n", [Inode,Name]),
	    case lookup_path_name_(ParentINode,State) of
		false -> false;
		"" -> Name;
		ParentName -> filename:join(ParentName, Name)
	    end
    end.


write_mode_(Filename, Mode) ->
    FI = #file_info { mode = Mode band ?S_IFMT },
    file:write_file_info(Filename, FI, [{time,posix}]).

-define(FUSE_SET_ATTR_MASK, 2#111111). %% FIXME! should be in fuserl?
write_stat_(Filename, Stat, Mask) ->
    FI = set_file_info(Stat, Mask band ?FUSE_SET_ATTR_MASK, #file_info {}),
    file:write_file_info(Filename, FI, [{time,posix}]).

set_file_info(_Stat, 0, FI) -> FI;
set_file_info(Stat, Mask, FI) when Mask band ?FUSE_SET_ATTR_MODE =/= 0 ->
    FI1 = FI#file_info { mode = Stat#stat.st_mode band ?S_IFMT },
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
    
    

%% read file stat
read_stat_(File) ->
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
	    {ok, #stat { st_ino   = FI#file_info.inode,
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

		     
datetime_to_unix(undefined) ->
    0;
datetime_to_unix(Posix) when is_integer(Posix) ->
    Posix;
datetime_to_unix(DateTime) ->
    Secs = calendar:datetime_to_gregorian_seconds(DateTime),
    Secs - 62167219200.
