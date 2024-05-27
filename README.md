[![progress-banner](https://backend.codecrafters.io/progress/redis/bb559497-c5fd-4e84-b125-d827d33f47a6)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

<h1>Disco Data</h1>
This a basic implementation of Redis in Python.

<h2>How to Run??</h2>
<p>Just run the this command</p>
```
exec python3 -m app.main "$@"

```


**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

```
Traceback (most recent call last):
  File "/.../python3.7/runpy.py", line 193, in _run_module_as_main
    "__main__", mod_spec)
  File "/.../python3.7/runpy.py", line 85, in _run_code
    exec(code, run_globals)
  File "/app/app/main.py", line 11, in <module>
    main()
  File "/app/app/main.py", line 6, in main
    s = socket.create_server(("localhost", 6379), reuse_port=True)
AttributeError: module 'socket' has no attribute 'create_server'
```

This is because `socket.create_server` was introduced in Python 3.8, and you
might be running an older version.

You can fix this by installing Python 3.8 locally and using that.

