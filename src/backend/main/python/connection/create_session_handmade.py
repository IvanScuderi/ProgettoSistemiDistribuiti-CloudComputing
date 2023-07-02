import json, pprint, requests, textwrap
host = 'http://localhost:8998'
data = {'kind': 'pyspark'}
headers = {'Content-Type': 'application/json'}
r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)

session_url = host + r.headers['location']
print(f"session_url -> {session_url}")
state = 'starting'
r = requests.get(session_url, headers=headers)
r = r.json()
print(f"state of the session -> {r['state']}, need to wait until idle...")
while state == 'starting':
  r = requests.get(session_url, headers=headers)
  r = r.json()
  state = r['state']
print(f"state of the session -> {r['state']}")

statements_url = session_url + '/statements'
print(f"statements_url -> {statements_url}")
data = {
  'code': textwrap.dedent("""
    import random
    NUM_SAMPLES = 100000
    def sample(p):
      x, y = random.random(), random.random()
      return 1 if x*x + y*y < 1 else 0

    count = sc.parallelize(range(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)
    print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
    """)
}

r = requests.post(statements_url, data=json.dumps(data), headers=headers)

r = requests.get(statements_url, headers=headers)
r = r.json()['statements'][0]
print(f"state of the results -> {r['state']}, need to wait until idle...")
while state != 'available':
  r = requests.get(statements_url, headers=headers)
  r = r.json()['statements'][0]
  state = r['state']
print(f"state of the results -> {r['state']}")

#stampo il risultato
print(r['output']['data']['text/plain'])

requests.delete(session_url, headers=headers)

