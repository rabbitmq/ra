SHELL := bash# we want bash behaviour in all shell invocations

.DEFAULT_GOAL = diagrams

.SUFFIXES: .ascii .png

ASCII=$(wildcard *.ascii)
PNG=$(ASCII:.ascii=.png)

### VARIABLES ###

### TARGETS ###
#

diagrams: ditaa-0.11.0-standalone.jar $(PNG) ## Build diagrams

clear-diagrams: $(PNG)
	rm $(PNG)

.ascii.png: ditaa-0.11.0-standalone.jar
	java -jar ditaa-0.11.0-standalone.jar --round-corners $< $@

ditaa-0.11.0-standalone.jar:
	wget https://github.com/stathissideris/ditaa/releases/download/v0.11.0/ditaa-0.11.0-standalone.jar