# This dockerfile utilizes components licensed by their respective owners/authors.
# Prior to utilizing this file or resulting images please review the respective licenses at: https://docs.python.org/3/license.html

FROM winamd64/python

LABEL Description="FLO2D Model" maintainer="hasitha.10@cse.mrt.ac.lk"

RUN powershell -command [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

WORKDIR /curw
RUN powershell -command Invoke-WebRequest -URI "https://www.flo-2d.com/wp-content/uploads/2012/08/FLO-2D-Pro-Model.zip"


CMD [ "cmd" ]
