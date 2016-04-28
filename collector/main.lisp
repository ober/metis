(in-package :ctcl)

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

#-allegro
(defun main ()
  ;;(ql:quickload :swank)
  (setq swank:*use-dedicated-output-stream* nil)
  ;;(swank:create-server :dont-close t :port 2221)
  ;;(declare (optimize (safety 3) (debug 3)))
  (format t "XXX: ~A~%" (type-of (argv)))
  (let* ((args (argv))
	 (verb (nth 1 args))
	 (workers (nth 2 args))
	 (dir (nth 3 args)))
    (cond
      ((equal "main" verb) (main))
      ((equal "a" verb)(time (cloudtrail-report-to-psql-async workers dir)))
      ((equal "s" verb)(time (cloudtrail-report-to-psql-sync dir)))
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
    ((equal "s" verb) (profile (ctcl::cloudtrail-report-to-psql-sync dir)))
    ((equal "a" verb) (profile (ctcl::cloudtrail-report-to-psql-async workers dir)))
    ((equal "r" verb)(time (run-bench)))
    (t (format t "Usage <~A> <p or s> <directory of logs>" app))))

(defun run-bench () 
  (ql:quickload :swank)
  (load "collector/load.lisp")
  (princ "XXX: Ensuring connections")
  (psql-ensure-connection "metis")
  ;;(princ "XXX: Dropping tables")
  (create-tables-psql "metis")
  (princ "XXX: Running Test")
  ;;(setq swank:*use-dedicated-output-stream* nil)
  ;;(swank:create-server :dont-close t :port 2221)
  #+sbcl (cl-user::profile "CTCL")
  #+allegro
  (progn
    ;;XX(setf excl:*tenured-bytes-limit* 524288000)
    (prof:with-profiling (:type :time) (ctcl::cloudtrail-report-to-psql-async "10" "~/test-ct/"))
    (prof:show-flat-profile))
  #-allegro
  (time (ctcl::cloudtrail-report-to-psql-async "10" "~/test-ct/"))
  )
