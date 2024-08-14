# Makefile for regression test

ROOT_DIR = ../..

include $(ROOT_DIR)/Makefile.global

# Just use the already installed version instead of installing to a temp
# location
with_temp_install =

check-regression-duckdb:
	TEST_DIR=$(CURDIR) $(pg_regress_check) \
	--temp-config regression.conf \
	--load-extension=pg_duckdb \
	--schedule schedule

clean-regression:
	rm -fr $(CURDIR)/tmp_check
	rm -fr $(CURDIR)/log
	rm -fr $(CURDIR)/results
	rm -f  $(CURDIR)/regression.diffs
	rm -f  $(CURDIR)/regression.out
