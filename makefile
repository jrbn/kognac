#Include variables dependant on the platform
include makefile.platform

LDFLAGS=-DBOOST_LOG_DYN_LINK -llz4 -lboost_filesystem -lboost_system -lboost_chrono -lboost_thread-mt -lboost_log-mt -lboost_log_setup-mt -lboost_program_options -lboost_iostreams -lpthread

CPPFLAGS=-c -MD -MF $(patsubst %.o,%.d,$@) -DBOOST_LOG_DYN_LINK -std=c++11 -I. -Iinclude

SRCDIR=src
O_FILES1=$(wildcard $(SRCDIR)/*.cpp) $(wildcard $(SRCDIR)/**/*.cpp) $(wildcard $(SRCDIR)/**/**/*.cpp)
O_FILES2=$(wildcard ext/*.cpp)

RELEASEFLAGS= -O3 -g -gdwarf-2 -DNDEBUG=1
BUILDIR_RELEASE=build

DEBUGFLAGS= -O0 -g -Wall -Wno-sign-compare -DDEBUG=1
BUILDIR_DEBUG=build_debug

PRG=kognac_exec
ifeq ($(DEBUG),1)
	CPPFLAGS+=$(DEBUGFLAGS)
	CFLAGS+=$(DEBUGFLAGS)
	BUILDIR=$(BUILDIR_DEBUG)
else
	CPPFLAGS+=$(RELEASEFLAGS)
	CFLAGS+=$(RELEASEFLAGS)
	BUILDIR=$(BUILDIR_RELEASE)
endif

BFILES1=$(subst $(SRCDIR),$(BUILDIR),$(O_FILES1:.cpp=.o))
BFILES_EXT=$(subst ext,$(BUILDIR)/ext,$(O_FILES2:.cpp=.o))

all: $(PRG)

$(PRG): init $(BFILES1) $(BFILES_EXT)
	$(CPLUS) -o $(BUILDIR)/$@ $(CLIBS) $(BFILES1) $(BFILES_EXT) $(LDFLAGS)
	ar rcs build/libkognac.a $(BFILES1) $(BFILES_EXT)

init:
	@mkdir -p $(BUILDIR)

# pull in dependency info for *existing* .o files
-include $(BFILES1:.o=.d)

$(BUILDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p `dirname $@`
	$(CPLUS) $(EXT_INCLUDE) $(CINCLUDES) $(CPPFLAGS) $< -o $@

$(BUILDIR)/ext/%.o: ext/%.cpp
	@mkdir -p `dirname $@`
	$(CPLUS) $(EXT_INCLUDE) $(CINCLUDES) $(CPPFLAGS) $< -o $@

clean:
	@rm -rf $(BUILDIR_RELEASE)
	@rm -rf $(BUILDIR_DEBUG)
	@echo "Cleaning completed"
