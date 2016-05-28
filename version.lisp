(defpackage :metis/version
  (:use :common-lisp :common-lisp :fare-memoization :cl-fad :gzip-stream :cl-json)
  (:import-from :yason)
  (:export #:get-hostname-by-ip
	   #:read-json-gzip-file))
   
(in-package :metis/version)

#+abcl (progn)

#+allegro (progn
	    ;;(setf excl:*tenured-bytes-limit* 52428800)
	    (setq excl:*global-gc-behavior* nil)
	    (setq excl:*load-source-file-info* nil)
	    (setq excl:*load-xref-info* nil)
	    (setq excl:*record-source-file-info* nil)
	    (setq excl:*record-xref-info* nil)
	    ;;(setf excl:*global-gc-behavior* :auto)
	    (eval-when (:compile-toplevel :load-toplevel :execute)
	      (require :acldns)))
#+clozure (progn
	    (ccl:set-lisp-heap-gc-threshold (ash 2 20)))
#+cmucl (progn
	  (setq ext:*bytes-consed-between-gcs* 25000000)
	  ;; to avoid problems when running the bignum code (the default of
	  ;; 40000 is too low for some of the tests)
	  (setq ext:*intexp-maximum-exponent* 100000))
#+ecl (progn
	(require 'cmp)
	(ext:set-limit 'ext:c-stack (* 8 1024 1024))
	#-ecl-bytecmp
	(setq c::*cc-flags* (concatenate 'string "-I. " c::*cc-flags*)))

#+lispworks (progn
	      (setq sys:*stack-overflow-behaviour* nil)
	      (hcl:toggle-source-debugging nil))
#+sbcl (progn
	 (eval-when (:compile-toplevel :load-toplevel :execute)
	   (require :sb-sprof)))

