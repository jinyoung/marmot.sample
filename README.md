## 설치 방법

### 1. 사전조건

* Oracle Java (Java8 이상) 설치되어 있어야 한다.
* [marmot.server.dist](https://github.com/kwlee0220/marmot.client.dist)를 사용할 수 있어야 한다.
* [marmot.client.dist](https://github.com/kwlee0220/marmot.client.dist) 프로젝트가 설치되어 있어야 한다.

### 2. 프로젝트 파일 다운로드 및 컴파일
`$HOME/marmot` 디렉토리를 만들어서 이동한다. 
<pre><code>$ cd $HOME/marmot</code></pre>

GitHub에서 `marmot.sample' 프로젝트를 download하고, 받은 zip 파일 (marmot.sample-master.zip)의
압축을 풀고, 디렉토리 이름을 `marmot.sample`로 변경한다.
* GitHub URL 주소: `https://github.com/kwlee0220/marmot.sample`
* 생성된  `marmot.sample` 디렉토리는 `$HOME/marmot/marmot.sample`에 위치한다.

생성된 디렉토리로 이동하여 컴파일을 시도한다.
<pre><code>$ cd marmot.sample
$ gradle copyJarToBin
</code></pre>

Eclipse IDE를 이용하려는 경우 `eclipse` Gradle task를 수행시켜 Eclipse 프로젝트 import에
필요한 `.project` 파일과 `.classpath` 파일을 생성시킨다.
<pre><code>$ gradle eclipse</code></pre>
