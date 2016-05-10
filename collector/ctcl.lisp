(in-package :ctcl)

(defvar *database* "metis-test")
(defvar *pcallers* 5)
(defvar *files* nil)
(defvar *h* (make-hash-table :test 'equalp))

#+allegro (setf excl:*tenured-bytes-limit* 52428800)
#+allegro (setf excl:*global-gc-behavior* :auto)
#+lispworks (setq sys:*stack-overflow-behaviour* nil)

;;(declaim (optimize (speed 3) (safety 0) (space 0)))
(defvar *mytasks* (list))

(defun have-we-seen-this-file (file)
  (format t ".")
  (let ((them (load-file-values)))
    (if (gethash (file-namestring file) them)
	t
	nil)))

(defun flatten (obj)
  (do* ((result (list obj))
        (node result))
       ((null node) (delete nil result))
    (cond ((consp (car node))
           (when (cdar node) (push (cdar node) (cdr node)))
           (setf (car node) (caar node)))
          (t (setf node (cdr node))))))


(defun walk-ct (path fn)
  (walk-directory path fn))

(defun sync-ct-file (x)
  (process-ct-file x))

(defun async-ct-file (x)
  (push (pcall:pexec
	 (funcall #'process-ct-file x)) *mytasks*))

(defun process-ct-file (x)
  (when (equal (pathname-type x) "gz")
    (unless (have-we-seen-this-file x)
      (db-mark-file-processed x)
      (format t "N")
      ;;(format t "New:~A~%" (file-namestring x))
      (parse-ct-contents x))))

(defun parse-ct-contents (x)
  (let ((records (cdr (elt (read-json-gzip-file x) 0))))
    (dolist (x records)
      (let* ((event-time (cdr-assoc :EVENT-TIME x))
	     ;;(user-identity (cdr-assoc :ACCESS-KEY-ID (cdr-assoc :USER-IDENTITY x)))
	     (event-name (cdr-assoc :EVENT-NAME x))
	     (user-agent (cdr-assoc :USER-AGENT x))
	     (ip (cdr-assoc :SOURCE-+IP+-ADDRESS x))
	     (hostname (get-hostname-by-ip ip))
	     (user-identity (cdr-assoc :USER-IDENTITY x))
	     (user-name (cdr-assoc :USER-NAME user-identity))
	     (user-key (cdr-assoc :ACCESS-KEY-ID user-identity)))
	(normalize-insert event-time user-name user-key event-name user-agent (or hostname ip))))))

(defun cloudtrail-report-sync (path)
  (let ((cloudtrail-reports (or path "~/CT")))
    (walk-ct cloudtrail-reports
	     #'sync-ct-file)))

(defun cloudtrail-report-async (workers path)
  (let ((workers (parse-integer workers)))
    (setf (pcall:thread-pool-size) workers)
    (let ((cloudtrail-reports (or path "~/CT")))
      (walk-ct cloudtrail-reports
	       #'async-ct-file))
    (ignore-errors (mapc #'pcall:join *mytasks*))))
