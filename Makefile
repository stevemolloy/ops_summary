summary.pdf: summary.py
	python summary.py | pandoc -t beamer -o summary.pdf
	#python summary.py

open: summary.pdf
	xdg-open summary.pdf
