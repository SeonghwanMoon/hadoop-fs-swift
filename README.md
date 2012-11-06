hadoop-fs-swift
===============

Swift filesystem for hadoop extracted from zerovm's hadoop-common repository https://github.com/zerovm/hadoop-common.

I have decided to provide the source as a stand-alone implementation in order to allow integration with newer versions
of hadoop. 

This version has been patched to allow for use with mapreduce, and integration with keystone using a modified version
of the java-cloudfiles repo provided by zmanda https://github.com/thinkdeciduous/java-cloudfiles.

To install:

1. download the latest stable hadoop build (tested with 1.0.4 to date).
2. copy all source files to ${hadoop-home}/src/core/org/apache/hadoop/fs/swift.
3. git clone https://github.com/thinkdeciduous/java-cloudfiles and build.
4. copy the freshly build java-cloudfiles.jar to ${hadoop-home}/lib/ along with the httpcore-4.1.3.jar, 
   httpclient-4.1.4.jar and org-json.jar.
5. modify your core-default.xml to include:&lt;br/>
   &lt;property&gt;<br/>
    &lt;name>fs.swift.impl&lt;/name&gt;<br/>
    &lt;value>org.apache.hadoop.fs.swift.SwiftFileSystem&lt;/value&gt;<br/>
    &lt;description>The FileSystem for swift: uris.&lt;/description&gt;<br/>
   &lt;/property&gt;<br/>
6. build hadoop
7. modify your core-site.xml to include:<br/>
   &lt;property&gt;<br/>
    &lt;name&gt;fs.swift.userName&lt;/name&gt;<br/>
    &lt;value&gt;[user]:[tenant]&lt;/value&gt;<br/>
   &lt;/property&gt;<br/>
   &lt;property&gt;<br/>
    &lt;name&gt;fs.swift.userPassword&lt;/name&gt;<br/>
    &lt;value&gt;[password]&lt;/value&gt;<br/>
   &lt;/property&gt;<br/>
   &lt;property&gt;<br/>
    &lt;name&gt;fs.swift.authUrl&lt;/name&gt;<br/>
    &lt;value&gt;http://[keystone-auth-ip]:5000/v2.0/tokens&lt;/value&gt;<br/>
   &lt;/property&gt;<br/>
   &lt;property&gt;<br/>
    &lt;name&gt;fs.default.name&lt;/name&gt;<br/>
    &lt;value&gt;swift://[swift-proxy-ip]:[swift-proxy-port]/v1/AUTH_[tenant-id-from-keystone]&lt;/value&gt;<br/>
   &lt;/property&gt;<br/>
   &lt;property&gt;<br/>
    &lt;name&gt;fs.swift.connectionTimeout&lt;/name&gt;<br/>
    &lt;value&gt;15000&lt;/value&gt;<br/>
   &lt;/property&gt;<br/>

