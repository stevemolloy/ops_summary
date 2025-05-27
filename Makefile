summary.pdf: summary.py
	python summary.py | pandoc -t beamer -o summary.pdf

open: summary.pdf
	xdg-open summary.pdf
