(in-package :metis)

#+allegro (progn
						(require :prof)
						(setq excl:*global-gc-behavior* nil)
						(setq excl:*load-source-file-info* nil)
						(setq excl:*load-xref-info* nil)
						(setq excl:*record-source-file-info* nil)
						(setq excl:*record-xref-info* nil)
						;;(setq excl:*tenured-bytes-limit* 5242880000)
						(eval-when (:compile-toplevel :load-toplevel :execute)
							(require :acldns)))

#+clozure (progn
						(ccl:set-lisp-heap-gc-threshold (ash 2 20)))

#+lispworks (progn
							(setq sys:*stack-overflow-behaviour* nil)
							(hcl:toggle-source-debugging nil))

#+sbcl (progn
				 (eval-when (:compile-toplevel :load-toplevel :execute)
					 (require :sb-sprof)))
