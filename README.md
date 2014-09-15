1. Install the App Engine SDK: https://developers.google.com/appengine/downloads
2. Locate the main App Engine SDK directory(`/usr/local/google_appengine`), which contains the development application server (dev_appserver.py).
3. Navigate to that directory or supply its path when invoking the application server.
4. Start the sample application in the application server providing any required path for both the application server and the sample application.
    - ``` dev_appserver.py mapreduce-made-easy/ ```
5. In your browser, start the web client for the sample application at this address:
http://localhost:8080/
6. When prompted, check "Sign in as Administrator" and login to the application (Google Account login):
7. Click Choose File and select the .zip file you want to process.