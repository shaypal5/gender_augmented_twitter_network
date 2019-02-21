"""Testing phase 4 functionalities."""

from twikwak17.phases.phase4 import uname_and_tweets_from_line


USERS = [' df9k', '  39048fd', '__asd7', 'bobo34', 'terk*#4']
TWEETS = [
    'i like my monogiri',
    'give me some sushi wait is derek sharp alive?',
    'some of that jazz is better just woke up%#',
    'between each two dots there is one line do you need white wine for pasta',
    'listen! oh my god! just what? already!*$! no you did nahhttt',
]


def test_uname_and_tweets_from_line():
    for i in range(len(USERS)):
        user = USERS[i]
        tweets = TWEETS[i]
        line = f'{user} {tweets}'
        ruser, rtweets = uname_and_tweets_from_line(line)
        assert ruser == user
        assert rtweets == tweets
