import requests

url = 'https://ip.smartproxy.com/json'
username = 'sp484h31o6'
password = 'ctozz4SWY55hoDsu9j'
proxy = f"https://{username}:{password}@ph.smartproxy.com:40000"
result = requests.get(url, proxies={
    'http': proxy,
    'https': proxy
})
print(result.text)
