#!/usr/bin/env python
#
# Copyright 2011 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" This is a sample application that tests the MapReduce API.

It does so by allowing users to upload a zip file containing plaintext files
and perform some kind of analysis upon it. Currently three types of MapReduce
jobs can be run over user-supplied input data: a WordCount MR that reports the
number of occurrences of each word, an Index MR that reports which file(s) each
word in the input corpus comes from, and a Phrase MR that finds statistically
improbably phrases for a given input file (this requires many input files in the
zip file to attain higher accuracies)."""

__author__ = """aizatsky@google.com (Mike Aizatsky), cbunch@google.com (Chris
Bunch)"""

import datetime
import jinja2
import logging
import re
import urllib
import webapp2

from google.appengine.ext import blobstore
from google.appengine.ext import db

from google.appengine.ext.webapp import blobstore_handlers

from google.appengine.api import files
from google.appengine.api import taskqueue
from google.appengine.api import users

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce import shuffler

from collections import Counter

NUM_SHARDS = 1

class FileMetadata(db.Model):
  """A helper class that will hold metadata for the user's blobs.

  Specifially, we want to keep track of who uploaded it, where they uploaded it
  from (right now they can only upload from their computer, but in the future
  urlfetch would be nice to add), and links to the results of their MR jobs. To
  enable our querying to scan over our input data, we store keys in the form
  'user/date/blob_key', where 'user' is the given user's e-mail address, 'date'
  is the date and time that they uploaded the item on, and 'blob_key'
  indicates the location in the Blobstore that the item can be found at. '/'
  is not the actual separator between these values - we use '..' since it is
  an illegal set of characters for an e-mail address to contain.
  """

  __SEP = ".."
  __NEXT = "./"

  owner = db.UserProperty()
  filename = db.StringProperty()
  uploadedOn = db.DateTimeProperty()
  source = db.StringProperty()
  blobkey = db.StringProperty()
  question_1_link = db.StringProperty()
  question_2_link = db.StringProperty()
  question_3_link = db.StringProperty()

  @staticmethod
  def getFirstKeyForUser(username):
    """Helper function that returns the first possible key a user could own.

    This is useful for table scanning, in conjunction with getLastKeyForUser.

    Args:
      username: The given user's e-mail address.
    Returns:
      The internal key representing the earliest possible key that a user could
      own (although the value of this key is not able to be used for actual
      user data).
    """

    return db.Key.from_path("FileMetadata", username + FileMetadata.__SEP)

  @staticmethod
  def getLastKeyForUser(username):
    """Helper function that returns the last possible key a user could own.

    This is useful for table scanning, in conjunction with getFirstKeyForUser.

    Args:
      username: The given user's e-mail address.
    Returns:
      The internal key representing the last possible key that a user could
      own (although the value of this key is not able to be used for actual
      user data).
    """

    return db.Key.from_path("FileMetadata", username + FileMetadata.__NEXT)

  @staticmethod
  def getKeyName(username, date, blob_key):
    """Returns the internal key for a particular item in the database.

    Our items are stored with keys of the form 'user/date/blob_key' ('/' is
    not the real separator, but __SEP is).

    Args:
      username: The given user's e-mail address.
      date: A datetime object representing the date and time that an input
        file was uploaded to this app.
      blob_key: The blob key corresponding to the location of the input file
        in the Blobstore.
    Returns:
      The internal key for the item specified by (username, date, blob_key).
    """

    sep = FileMetadata.__SEP
    return str(username + sep + str(date) + sep + blob_key)


class IndexHandler(webapp2.RequestHandler):
  """The main page that users will interact with, which presents users with
  the ability to upload new data or run MapReduce jobs on their existing data.
  """

  template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"),
                                    autoescape=True)

  def get(self):
    user = users.get_current_user()
    username = user.nickname()

    first = FileMetadata.getFirstKeyForUser(username)
    last = FileMetadata.getLastKeyForUser(username)

    q = FileMetadata.all()
    q.filter("__key__ >", first)
    q.filter("__key__ < ", last)
    results = q.fetch(10)

    items = [result for result in results]
    length = len(items)

    upload_url = blobstore.create_upload_url("/upload")

    self.response.out.write(self.template_env.get_template("index.html").render(
        {"username": username,
         "items": items,
         "length": length,
         "upload_url": upload_url}))

  def post(self):
    filekey = self.request.get("filekey")
    blob_key = self.request.get("blobkey")

    if self.request.get("question_1"):
      pipeline = SISL2MostPopularPipeline(filekey, blob_key)
    elif self.request.get("question_2"):
      pipeline = SISL2MaxUsersPipeline(filekey, blob_key)
    elif self.request.get("question_3"):
      pipeline = SISL3MostPopularHr(filekey, blob_key)

    pipeline.start()
    self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)

SIS_L2_CLSRMS = ['1010200068', '1010200069', '1010200070', '1010200071', '1010200072', '1010200073', '1010200074', '1010200075', '1010200076', '1010200077', '1010200078', '1010200079', '1010200080', '1010200081', '1010200082', '1010200089', '1010200090', '1010200091', '1010200092', '1010200093', '1010200094', '1010200095', '1010200096', '1010200097', '1010200098', '1010200099', '1010200100', '1010200101', '1010200102', '1010200103', '1010200104', '1010200105', '1010200106', '1010200107', '1010200108', '1010200109', '1010200110', '1010200111', '1010200112', '1010200113', '1010200114', '1010200115', '1010200116', '1010200117', '1010200118', '1010200119', '1010200120', '1010200121', '1010200122', '1010200123', '1010200124', '1010200125']
SIS_L2_CLSRMS_NAME = ['SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-4', 'SMUSISL2SR2-3', 'SMUSISL2SR2-3', 'SMUSISL2SR2-3', 'SMUSISL2SR2-3', 'SMUSISL2SR2-3', 'SMUSISL2SR2-3', 'SMUSISL2SR2-3', 'SMUSISL2SR2-3', 'SMUSISL2SR2-3', 'SMUSISL2SR2-3', 'SMUSISL2SR2-3', 'SMUSISL2SR2-2', 'SMUSISL2SR2-2', 'SMUSISL2SR2-2', 'SMUSISL2SR2-2', 'SMUSISL2SR2-2', 'SMUSISL2SR2-2', 'SMUSISL2SR2-2', 'SMUSISL2SR2-2', 'SMUSISL2SR2-2', 'SMUSISL2SR2-2', 'SMUSISL2SR2-2', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1', 'SMUSISL2SR2-1']

def sisl2_most_popular_map(data):
  """Determine most popular SIS LR classroom map function."""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Got %s", entry.filename)
  # e.g. 21/2/14 15:26  01a70ae4514784ba803a527856eb4fbfc9080978  1010200074  99.9981 3 3
  for record in text.split("\n"):
    data = record.split(",")
    try:
      timestamp = data[0]
      mac_id = data[1]
      location_id = data[2]
    except IndexError:
      continue #line was empty

    if timestamp[11:13] == "12" and location_id in SIS_L2_CLSRMS:
      room_name = SIS_L2_CLSRMS_NAME[SIS_L2_CLSRMS.index(location_id)]
      yield (room_name, mac_id)

def sisl2_most_popular_reduce(key, values):
  """Determine most popular SIS LR classroom reduce function."""
  yield "%s: %d\n" % (key, len(set(values)))


def sisl2_max_user_map(data):
  """Determine SIS L2 classroom with max users per day map function."""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Got %s", entry.filename)
  # e.g. 2014-02-01 00:00:38,34faeb58d58db27491c85ba8e683c0cc6764dc84,1010400001,-9900,3,3
  for record in text.split("\n"):
    data = record.split(",")
    try:
      timestamp = data[0]
      mac_id = data[1]
      location_id = data[2]
    except IndexError:
      continue #line was empty

    if timestamp[11:13] == "12" and location_id in SIS_L2_CLSRMS:
      date = timestamp[0:10]
      room_name = SIS_L2_CLSRMS_NAME[SIS_L2_CLSRMS.index(location_id)]
      yield (date, room_name + "," + mac_id)

def sisl2_max_user_reduce(key, values):
  """Determine SIS L2 classroom with max users per day reduce function."""
  unique_room_user = set(values) # to get unique users in each room in each day

  room = []
  for room_user in unique_room_user:
    room.append(room_user[0:room_user.index(",")]) #we just want the room

  max_users = Counter(room).most_common(1)[0]

  yield "%s: %s\n" % (key, max_users[0] + " - " + str(max_users[1]))

SIS_L3_CLSRM31 = ['1010300123', '1010300124', '1010300125', '1010300126', '1010300127', '1010300128', '1010300129', '1010300130', '1010300131', '1010300132', '1010300133', '1010300134', '1010300135', '1010300136', '1010300137']

def sisl3_most_popular_hour_map(data):
  """Determine most popular hour of SIS L3 CR function."""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Got %s", entry.filename)
  # e.g. 2014-02-01 00:00:38,34faeb58d58db27491c85ba8e683c0cc6764dc84,1010400001,-9900,3,3
  for record in text.split("\n"):
    data = record.split(",")
    try:
      timestamp = data[0]
      mac_id = data[1]
      location_id = data[2]
    except IndexError:
      continue #line was empty

    if location_id in SIS_L3_CLSRM31:
      hour = timestamp[11:13]
      yield (hour, mac_id)

def sisl3_most_popular_hour_reduce(key, values):
  """Determine most popular hour of SIS L3 CR reduce function."""
  yield "%s: %d\n" % (key, len(set(values)))

class SISL2MostPopularPipeline(base_handler.PipelineBase):
  """A pipeline to determine which SIS level 2 classroom is the most popular
  between 12:00:00 and 13:00:00.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      csv files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    output = yield mapreduce_pipeline.MapreducePipeline(
        "question_1",
        "main.sisl2_most_popular_map",
        "main.sisl2_most_popular_reduce",
        "mapreduce.input_readers.BlobstoreZipInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params={
            "blob_key": blobkey,
        },
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=NUM_SHARDS)
    yield StoreOutput("Question1", filekey, output)

class SISL2MaxUsersPipeline(base_handler.PipelineBase):
  """A pipeline to determine which SIS level 2 classroom has the highest
  number of users per day from 12:00:00 to 13:00:00.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      csv files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    output = yield mapreduce_pipeline.MapreducePipeline(
        "question_2",
        "main.sisl2_max_user_map",
        "main.sisl2_max_user_reduce",
        "mapreduce.input_readers.BlobstoreZipInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params={
            "blob_key": blobkey,
        },
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=NUM_SHARDS)
    yield StoreOutput("Question2", filekey, output)

class SISL3MostPopularHr(base_handler.PipelineBase):
  """A pipeline to determine which hour the SIS Level 3 classroom 3-1 is the
  most popular.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      csv files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    output = yield mapreduce_pipeline.MapreducePipeline(
        "question_3",
        "main.sisl3_most_popular_hour_map",
        "main.sisl3_most_popular_hour_reduce",
        "mapreduce.input_readers.BlobstoreZipInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params={
            "blob_key": blobkey,
        },
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=NUM_SHARDS)
    yield StoreOutput("Question3", filekey, output)

class StoreOutput(base_handler.PipelineBase):
  """A pipeline to store the result of the MapReduce job in the database.

  Args:
    mr_type: the type of mapreduce job run (e.g., WordCount, Index)
    encoded_key: the DB key corresponding to the metadata of this job
    output: the blobstore location where the output of the job is stored
  """

  def run(self, mr_type, encoded_key, output):
    logging.debug("output is %s" % str(output))
    key = db.Key(encoded=encoded_key)
    m = FileMetadata.get(key)

    if mr_type == "Question1":
      m.question_1_link = output[0]
    elif mr_type == "Question2":
      m.question_2_link = output[0]
    elif mr_type == "Question3":
      m.question_3_link = output[0]

    m.put()

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
  """Handler to upload data to blobstore."""

  def post(self):
    source = "uploaded by user"
    upload_files = self.get_uploads("file")
    blob_key = upload_files[0].key()
    name = self.request.get("name")

    user = users.get_current_user()

    username = user.nickname()
    date = datetime.datetime.now()
    str_blob_key = str(blob_key)
    key = FileMetadata.getKeyName(username, date, str_blob_key)

    m = FileMetadata(key_name = key)
    m.owner = user
    m.filename = name
    m.uploadedOn = date
    m.source = source
    m.blobkey = str_blob_key
    m.put()

    self.redirect("/")


class DownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """Handler to download blob by blobkey."""

  def get(self, key):
    key = str(urllib.unquote(key)).strip()
    logging.debug("key is %s" % key)
    blob_info = blobstore.BlobInfo.get(key)
    self.send_blob(blob_info)


app = webapp2.WSGIApplication(
    [
        ('/', IndexHandler),
        ('/upload', UploadHandler),
        (r'/blobstore/(.*)', DownloadHandler),
    ],
    debug=True)
