(in-package :ctcl)

#+sbcl
(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-sprof))

#+allegro
(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :acldns))

(defun argv ()
  (or
   #+clisp (ext:argv)
   #+sbcl sb-ext:*posix-argv*
   #+abcl ext:*command-line-argument-list*
   #+clozure (ccl::command-line-arguments)
   #+gcl si:*command-args*
   #+ecl (loop for i from 0 below (si:argc) collect (si:argv i))
   #+cmu extensions:*command-line-strings*
   #+allegro (sys:command-line-arguments)
   #+lispworks sys:*line-arguments-list*
   nil))

(defun do-bench ()
  (cloudtrail-report-async "10" "~/test-ct/"))

(defun run-bench () 
  ;;(load "collector/load.lisp")
  (princ "XXX: Ensuring connections")
  (db-ensure-connection "metis-test")
  ;;(princ "XXX: Dropping tables")
  (db-create-tables)
  (princ "XXX: Running Test")
  #+sbcl
  (time (sb-sprof:with-profiling (:report :flat) (do-bench)))
  #+lispworks
  (progn
    (hcl:set-up-profiler :package '(ctcl))
    (hcl:profile (cloudtrail-report-async "10" "~/test-ct/")))
  #+allegro
  (progn
    (setf excl:*tenured-bytes-limit* 524288000)
    (prof::with-profiling (:type :space) (time (do-bench)))
    (prof::show-flat-profile))
  #+ccl
  (time (cloudtrail-report-async "10" "~/test-ct/"))
  )

#-allegro
(defun main ()
  ;;(declare (optimize (safety 3) (debug 3)))
  (format t "XXX: ~A~%" (type-of (argv)))
  (let* ((args (argv))
	 (verb (nth 1 args))
	 (workers (nth 2 args))
	 (dir (nth 3 args)))
    (cond
      ((equal "main" verb) (main))
      ((equal "a" verb)(time (cloudtrail-report-async workers dir)))
      ((equal "s" verb)(time (cloudtrail-report-sync dir)))
      ((equal "r" verb)(time (run-bench)))
      (t (progn
	   (format t "Usage: <~A> <function> <args>~%" (nth 0 args))
	   (format t "Function is (s) for single threaded, and (a) for multithreaded~%")
	   (format t "ex: ~A a 10 ~~/CT/ # Would run 10 works on ~~/CT/~%" (nth 0 args))
	   (format t "ex: ~A s ~~CT/ # Would run 10 works on ~~/CT/~%" (nth 0 args)))))))

#+allegro
(in-package :cl-user)

#+allegro
(defun main (app verb workers dir)
  (format t "Got: app:~A verb:~A workers:~A dir:~A~%" app verb workers dir)
  (cond
    ((equal "s" verb) (profile (ctcl::cloudtrail-report-sync dir)))
    ((equal "a" verb) (profile (ctcl::cloudtrail-report-async workers dir)))
    ((equal "r" verb)(time (ctcl::run-bench)))
    (t (format t "Usage <~A> <p or s> <directory of logs>" app))))
