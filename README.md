[![N|Solid](https://www.riodb.org/images/Logo_Name_Small.jpg)](https://www.riodb.org/index.html)

RioDB (www.riodb.org) is a downloadable software for data stream processing.  
You can build powerful data screening bots through simple SQL-like statements.

## In a nuthshell:

- You define streams that will receive data using plugins (udp, tcp, http, etc.)
- You define windows of data (example: messages received in the last 2 hours that contain the word 'free'.)
- You write queries against these windows, similarly to how you query tables in a database.
- Your queries also indicate what action to take when conditions are met.
- RioDB takes actions by using plugins (udp, tcp, http, etc.)

RioDB is ultra lightweight and fast so that you can crunch through a lot of data using very little compute resources.
Plugins are also open source and anyone can make their own custom plugin if they want to. 

## Installation

RioDB requires OpenJDK (builds are tested using 14.0.2). JDK is required because your queries are compiled into efficient  programs at runtime.  
Please refer to:  

[Installation Instructions](https://www.riodb.org/Installation.html)
[Quick Start Guide](https://www.riodb.org/qstart.html)

## Plugins

RioDB plugins bundled with RioDB downloadable archive are maintained in a separate Git-hub repository:
https://github.com/RioDB/pluginExamples


## Development

Want to join the cause? Awesome!  
RioDB is developed in Java. The core engine is licensed under GPLv3.0, and the plugins are licensed under Apache 2.0.  
You don't need to be a Java programmer to help. We also seek input from data scientiests and devops engineers as far as desired features, math functions, software packaging, etc.  
The best place to start is probably our [Discord server](https://discord.gg/FbjRHstSkV) 

## Credits

RioDB uses Maven to import the following Java artifacts:  
[JCTools](https://github.com/JCTools/JCTools)
[log4j](https://gitbox.apache.org/repos/asf?p=logging-log4j2.git)
[InMemoryJavaCompiler](https://github.com/trung/InMemoryJavaCompiler)
[Guava](https://github.com/google/guava)
