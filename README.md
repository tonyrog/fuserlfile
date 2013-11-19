fuserlfile
==========

demo of fuserl (Erlang FUSE binding) which provides a 
pass through filesystem.

# compile

    rebar compile

# run

    [ do once 
      cp sys.config to local.config
      <edit> local.config
    ]
    erl -config local.config -s fuserlfile

Now you should be able to peek into the $HOME/erlfile directory and
be able to inspect a local root directory.
