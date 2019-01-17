twikwak17: A gender-augmented Twitter network dataset
#####################################################

This repository contains *twikwak17*, a gender-augmented Twitter network dataset, the code to generate it from two specific existing Twitter datasets, and an overview of both.

.. contents:: Table of Contents

.. section-numbering::



The Dataset
===========

Etymology
---------

The generation process of this dataset depends on two existing datasets, *kwak10www* and *twitter7* (see `Dependencies`_ for more information), and so the amelgamation of both names into *twikwak17* was chosen.


Stats
-----

* The number of nodes in the dataset (as it is the number if users in the intersection of the *twitter7* dataset and the *kwak10www* dataset) is 2,996,026.

Node intersection, resulting network size, etc..


Generation
==========

Dependencies
------------

* `Kwak10www <http://an.kaist.ac.kr/traces/WWW2010.html>`_ - The social graph component, consisting of 41.7 million user profiles and 1.47 billion social relations,  collected between July 6th, 2009 to July 31st, 2009. Created for and used in the `"What is Twitter, a Social Network or a News Media?" paper <http://an.kaist.ac.kr/traces/WWW2010.html>`_.

* `twitter7 <http://snap.stanford.edu/data/twitter7.html>`_ - A dataset consisting of nearly 580 million Twitter posts from 20 million users covering a 8 month period from June 2009 to February 2010. Estimated to be about 20-30% of all posts published on Twitter during that time frame. Created as part of [`J. Yang, J. Leskovec. Temporal Variation in Online Media. ACM International Conference on Web Search and Data Mining (WSDM '11), 2011. <http://ilpubs.stanford.edu:8090/984/1/paper-memeshapes.pdf>`_].

* `SPEKS <https://github.com/shaypal5/speks>`_ - Text-based gender prediction for Twitter in Python (a Python 3 packaging I wrote for other people's code).


Process
-------

The generation process is composed of several stages:

1. The first phase reads through the *twitter7* dataset. Since this dataset consists of chronologically-ordered tweets, this phase uses a single pass to construct two intermediate resources required for the generation process:

   a) A lexicographically sorted list of all usernames whose tweets are included in the dataset.
   b) A lexicographically sorted, user-wise merging of the tweets in the dataset, resulting in a set of files in the following format:

   .. code-block:: python

     u x1,1 x1,2 ... xn,1 ... xn,m

   Where ``u`` is the username (or Twitter handle) of the user, ``x1,1`` is the first word of the first tweet by him encoutered in the pass, ``x1,2`` is the second word of that tweet, ``xn,1`` is the first word of the last (n-th) tweet by him encoutered in the pass and ``xn,m`` is the last (m-th) word in that last tweet. An example line, for a user with the handle ``britishcoala`` is:
   
   .. code-block:: python

     britishcoala i like donuts have you seen the game yesterday ... i'm closing my tweeter account !
     
   The resulting format, where all tweets by a single user are concatenated into a single line, seperated by single whitespaces, matches the input format of a piece of code down the line in the process

   The sub-phases for this phase are:

   1.1. Reading through the 7 tweets file in order, loading tweets into user-to-tweets in-memory map (merging indvidual tweets by the same user); this map is offloaded to disk whenever memory is closed to be full. In each such offloading, the

   1.2. Merge-sorting the sorted username files into a single sorted username list file named ``twitter7_user_list.txt.gz``.

   1.3. Merge-sorting the sorted username-to-tweets files into a single sorted username-to-tweets file named ``twitter7_tweet_list.txt.gz``.
  
2. The second phase reads through the ``numeric2screen.tar.gz`` file of the *kwak10www* dataset and produces a lexicographically sorted handle-to-numeric-id mapping of the users in the dataset. The sub-phases are:

   2.1. Inverting ``numeric2screen`` into several lexicographically sorted username-to-id list files.

   2.2. Sort-merging the sorted username-to-id files into a single sorted username-to-id file named ``kwak10_uname_to_id.txt.gz`` and a single sorted username list named ``kwak10_unames.txt.gz``.

3. The third stage merges the two sorted lists of user handles (``twitter7_user_list.txt.gz`` and ``kwak10_unames.txt.gz``) to create a lexicographically sorted list of the intersection between the two lists. 

4. The fourth stage runs each line - in the user-wise merged tweets files - belonging to a user in the intersection list through the `SPEKS gender predictor for Twitter <https://github.com/shaypal5/speks>`_, and generates a lexicographically sorted user-handle-to-gender mapping.

5. The fifth stage uses the aforementioned handle-to-numeric-id mapping to transform the user-handle-to-gender mapping into a user-id-to-gender mapping.

6. Finally, the sixth stage runs through the social graph file of the *kwak10www* dataset (``twitter_rv.zip``) and removes any links/edges where at least one of the nodes is not the intersection list.


Complexity
----------

Define ``l7`` to be the number of lines in the *twitter7* dataset and ``u7`` to be the number of users in it. Define ``u10`` to be the number of users in the *kwak10www* dataset and ``l10`` the number of lines (i.e. edges) in it. Finally, define ``u`` to be the number of users in the intersection of both user lists.

1. Phase 1 runs in :math:`O(u7 log(u7)+l7+u7) ~ O(u7 log(u7))`, as it reads through ``l7`` lines once, and writes ``u7`` lines to disk.

2. Phase 2 runs in :math:`O(u10 log(u10))`, as it reads through ``u10`` lines once, sorts them in-memory in :math:`O(u10 log u10)` and writes ``u10`` lines.

3. Phase 3 runs in :math:`O(u7+u10)`, as it merges two sorted lists in time :math:`O(u7+u10)` and write ``u`` lines to disk.

4. Phase 4 runs in :math:`O(u)`, as it calls the gender prediction algorithm ``u`` times and writes ``u`` lines to disk.

5. Phase 5 runs in :math:`O(u)`, as it performs a single pass through a ``u``-lines-long file and writes ``u`` lines to disk.

6. Phase 6 runs in :math:`O(l10 * log(u))`, as it reads ``l10`` lines, performs ``l1`` searches in a ``u``-sized hash table, and writes ``l10`` lines to disk.


License
=======

The code in this repository is released under the `MIT license <https://choosealicense.com/licenses/mit/>`_.

The dataset itself is released under the `CC BY-SA 4.0 license <https://creativecommons.org/licenses/by-sa/4.0/>`_.
