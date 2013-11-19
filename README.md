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

Now you shoule be able to peek into the $HOME/erlfile and
be able to inspect the Erlang a local directory in an other location.
