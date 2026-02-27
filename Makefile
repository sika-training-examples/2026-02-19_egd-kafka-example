build-and-push-all:
	@make -C producer build-and-push
	@make -C processor build-and-push
	@make -C consumer build-and-push
