
programs = \
	target/release/hashtable_shm_client \
	target/release/hashtable_shm_server


.PHONY: all
all: $(programs)

$(programs):
	cargo build --release

.PHONY: clean
clean::
	${RM} -rf target
