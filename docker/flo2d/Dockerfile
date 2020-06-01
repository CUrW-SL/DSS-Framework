# This dockerfile utilizes components licensed by their respective owners/authors.
# Prior to utilizing this file or resulting images please review the respective licenses at: https://docs.python.org/3/license.html

FROM winamd64/python

LABEL Description="FLO2D Model" maintainer="hasitha.10@cse.mrt.ac.lk"

ENV GIT_VERSION 2.15.1
ENV GIT_PATCH_VERSION 2

RUN powershell -Command $ErrorActionPreference = 'Stop' ; \
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 ; \
    Invoke-WebRequest $('https://github.com/git-for-windows/git/releases/download/v{0}.windows.{1}/MinGit-{0}.{1}-busybox-64-bit.zip' -f $env:GIT_VERSION, $env:GIT_PATCH_VERSION) -OutFile 'mingit.zip' -UseBasicParsing ; \
    Expand-Archive mingit.zip -DestinationPath c:\mingit ; \
    Remove-Item mingit.zip -Force ; \
    setx /M PATH $('c:\mingit\cmd;{0}' -f $env:PATH)

RUN powershell -command wget -Uri "https://www.flo-2d.com/wp-content/uploads/2012/08/FLO-2D-Pro-Model.zip"
#RUN ["powershell", "-command", "Execute-MyCmdlet", "-param1 \"c:\\foo.txt\""]

RUN powershell -command wget -Uri "https://github.com/git-for-windows/git/releases/download/v2.26.2.windows.1/Git-2.26.2-64-bit.exe"

CMD [ "cmd" ]
