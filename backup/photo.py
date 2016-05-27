#/usr/bin/python
# coding=utf-8

import os
import httplib, httplib2
import simplejson
import time 


#httplib2.debuglevel = 1

def upload(host, port, uri, file_path):
    with open(file_path, 'rb') as f:
        conn=None
        try:
            headers = {}
            length = os.fstat(f.fileno()).st_size
            headers['Content-Length'] = length
            headers['Content-Type'] = 'image/jpeg;charset=UTF-8'
            conn = httplib.HTTPConnection(host, port, timeout=2000)
            conn.request('PUT', uri, f, headers)
            response = conn.getresponse()
            print response.read()
            status = response.status
            if status >= 200 and status < 300:
                content = response.read()
                print content
                return simplejson.loads(content)['resource_id']
            else:
                print response.status
                print response.reason
                print response.read()
        finally:
            if conn:
                conn.close()

   

def download(uri=None):
    conn = None
    with open('/home/balaamwe/download.jpg', 'wb') as f:

        conn = httplib2.Http()
        resp, content = conn.request(uri, 'GET')
	print resp['status'],resp.get('content-location', 'content-location not found')
	if resp['status'] == '200' and resp['content-type'] == 'application/json;charset=UTF-8':
	    print content
        f.write(content)

def update(url=None):
    conn = httplib2.Http()
    resp, content = conn.request(url, 'POST', headers={'content-type':'application/x-www-form-urlencoded;charset=UTF-8'})
    print resp

def delete(url=None):
    conn = httplib2.Http()
    resp, content = conn.request(url, 'DELETE')
    print content


if __name__ == '__main__':
    types=101
    #host = 'resource.youja.cn'
    #host = '122.144.133.40'
    #port = 30018
    #host = '10.10.10.21'
    #port = 8081
    host = 'resource.youja.cn'
    #host = '127.0.0.1'
    port = 80
    #port = 8082
    #host = '127.0.0.1'
    #port = 8009
    #owner='14179031'
    owner='111111'
    admin='10000'
    public='10001'

    print "###PUT###"
    for i in range(1,2):
	#upload_uri = '/uplusmain-file/resource_type/101?receiverid=sd&albumid=0&frameid=0&desc=null&optype=0&user_type=3&client_ver=4.0.3-g&token=s00e385000010c429c0e92c96598f5181884f58935a25ebff100b19&from=client&ua=uplus-android-HTC+802d&user_id=14179031'
	upload_uri='/uplusmain-file/resource_type/101?albumid=0&frameid=0&desc=null&optype=1&user_type=3&client_ver=4.0.1-g&token=s00e330000010c43d8ef768417140ca20ce417ba75c41be1c304cdda55efd28791048199c2b99261a0a1149&from=client&ua=zhihuiyun%230008-android-HUAWEI+Y600-U00&user_id=14179031&language=zh-CN'
	#upload_uri='/uplusmain-file/resource_type/101?albumid=0&frameid=0&desc=null&optype=2&user_type=3&client_ver=4.0.1-g&token=s00e385000010c429c0e92c96598f5181884f58935a25ebff100b19&from=client&ua=tengxun%23000c-android-G620-L75&user_id=14179031&language=zh-CN'
	#print upload_uri
	#upload_uri='/uplusmain-file/resource_type/101?frameid=0&user_id=46699083&user_type=7&from=client&desc=&ua=iphoneep%2300x2-iphone&client_ver=4.0.3.150415&token=t00d322000010c41c9a358edf3bfcee6db44193bb6035493bc94bb2&optype=0&language=zh-CN'
        #upload_uri = 'http://resource.api.youja.cn/uplusmain-file/resource_type/101?frameid=0&user_id=46699083&user_type=7&desc=null&ua=iphoneep%2300x2-iphone&client_ver=4.0.3.150415&token=t00d322000010c41c9a358edf3bfcee6db44193bb6035493bc94bb2&optype=0'
    	#resource_id = upload(host, port, upload_uri, '/home/balaamwe/%d.jpg' % i ) 
    	resource_id = upload(host, port, upload_uri, '/Users/chenlu/Downloads/15243787_235615180184_2.jpg') 
    	#delete_url='http://%s:%d/resource_type/101/resource_id/%d?user_id=14179031&token=10000' % (host, port, int(resource_id))
    	#delete(url=delete_url)
    #update("http://127.0.0.1:8082/uplusmain-file/resource_type/101/resource_id/424973290?frameid=0&user_id=14179031&user_type=7&resource_type=101&ua=appstore%2300bt-iphone&client_ver=4.0.0.150211&resource_id=http://resource.api.youja.cn/resource_type/101/resource_id/424973290&optype=2&albumid=0&token=t00e356000010b487d03ee91fe2ef50708b486aac1dd0ffee0fb897&language=zh-CN");
    
    
    exit(0)
    print "__________________________________________________________________--"
    download_url = 'http://%s:%d/uplusmain-file/resource_type/%s/resource_id/%d?user_id=%s' % (host, int(port),str(types),int(resource_id),str(owner))
    print "###owner GET##%s"  % download_url
    starttime = time.time()*1000 
    download(uri=download_url)
    endtime = time.time()*1000
    print (endtime - starttime) 
    starttime = time.time()*1000 
    download(uri=download_url)
    endtime = time.time()*1000
    print (endtime - starttime) 
    starttime = time.time()*1000 
    download(uri=download_url)
    endtime = time.time()*1000
    print (endtime - starttime) 
    exit(0)

    download_url = 'http://%s:%d/resource_type/%s/resource_id/%d?user_id=%s' % (host, int(port),str(types),int(resource_id),str(public))
    print "###public GET##%s" % download_url
    download(uri=download_url)

    download_url = 'http://%s:%d/resource_type/%s/resource_id/%d?user_id=%s' % (host, int(port),str(types),int(resource_id),str(admin))
    print "###admin GET##%s" % download_url
    download(uri=download_url)

    print "__________________________________________________________________--"
    delete_url='http://%s:%d/resource_type/101/resource_id/%d?user_id=14179031&token=10000' % (host, port, int(resource_id))
    delete(url=delete_url)

    download_url = 'http://%s:%d/resource_type/%s/resource_id/%d?user_id=%s' % (host, int(port),str(types),int(resource_id),str(owner))
    print "###owner GET##%s"  % download_url
    download(uri=download_url)

    download_url = 'http://%s:%d/resource_type/%s/resource_id/%d?user_id=%s' % (host, int(port),str(types),int(resource_id),str(public))
    print "###public GET##%s" % download_url
    download(uri=download_url)

    download_url = 'http://%s:%d/resource_type/%s/resource_id/%d?user_id=%s' % (host, int(port),str(types),int(resource_id),str(admin))
    print "###admin GET##%s" % download_url
    download(uri=download_url)

    print "__________________________________________________________________--"
    delete_url='http://%s:%d/resource_type/101/resource_id/%d?user_id=10000&token=10000&delete=1' % (host, port, int(resource_id))
    delete(url=delete_url)

    download_url = 'http://%s:%d/resource_type/%s/resource_id/%d?user_id=%s' % (host, int(port),str(types),int(resource_id),str(owner))
    print "###owner GET##%s"  % download_url
    download(uri=download_url)

    download_url = 'http://%s:%d/resource_type/%s/resource_id/%d?user_id=%s' % (host, int(port),str(types),int(resource_id),str(public))
    print "###public GET##%s" % download_url
    download(uri=download_url)

    download_url = 'http://%s:%d/resource_type/%s/resource_id/%d?user_id=%s' % (host, int(port),str(types),int(resource_id),str(admin))
    print "###admin GET##%s" % download_url
    download(uri=download_url)
