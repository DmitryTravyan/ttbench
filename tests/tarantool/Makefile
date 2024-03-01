.PHONY: dev-recreate test

dev-clean:
	cartridge stop
	sleep 3
	cartridge clean

dev-build:
	cartridge build

dev-start:
	cartridge start -d

dev-bootstrap:
	cartridge replicasets setup --bootstrap-vshard --debug --verbose

dev-apply-migration:
	ansible-playbook -i hosts.yml playbook.yml

dev-recreate: dev-clean dev-build dev-start dev-bootstrap
	cartridge log --follow --lines 1000

dev-recreate-without-build: dev-clean dev-start dev-bootstrap
	cartridge log --follow --lines 1000

dev-lint:
	.rocks/bin/luacheck .

dev-test:
	.rocks/bin/luatest test/integration -v -f -c

dev-test-all: dev-test-deps dev-lint dev-test
