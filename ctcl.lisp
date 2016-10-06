(in-package :metis)

(defvar *mytasks* (list))

(defun have-we-seen-this-file (file)
  (let ((them (load-file-values)))
    (if (gethash (file-namestring file) them)
  	t
	nil)))

;; (defun have-we-seen-this-value (var value)
;;   (let ((them (load-values var)))
;;     (if (gethash var them)
;;   	t
;; 	nil)))

(defun walk-ct (path fn)
  (cl-fad:walk-directory path fn))

(defun sync-ct-file (x)
  (process-ct-file x))

(defun async-ct-file (x)
  (push (pcall:pexec
	  (funcall #'process-ct-file x)) *mytasks*))

(defun process-ct-file (x)
  (when (equal (pathname-type x) "gz")
    (unless (have-we-seen-this-file x)
      (db-mark-file-processed x)
      (parse-ct-contents x))))


(defun fetch-value (indicators plist)
  "Return the value at the end of the indicators list"
  (reduce #'getf indicators :initial-value plist))

(defun parse-ct-contents (x)
  (let* ((records (second (read-json-gzip-file x)))
	 (num (length records))
	 (btime (get-internal-real-time)))
    (dolist (x records)
      (let* ((event-time (getf x :|eventTime|))
	     ;;(user-identity (cdr-assoc :ACCESS-KEY-ID (cdr-assoc :USER-IDENTITY x)))
	     (event-name (getf x :|eventName|))
	     (user-agent (getf x :|userAgent|))
	     (ip (getf x :|sourceIPAddress|))
	     (hostname (get-hostname-by-ip ip))
	     (user-identity (getf x :|userIdentity|))
	     (user-name (fetch-value '(:|userIdentity| :|sessionContext| :|sessionIssuer| :|userName|) x))
	     (user-key (fetch-value '(:|userIdentity| :|accessKeyId|) x)))
	(normalize-insert event-time user-name user-key event-name user-agent (or hostname ip))))
    (let* ((etime (get-internal-real-time))
	   (delta (/ (float (- etime btime)) (float internal-time-units-per-second))))
      (if (and (> delta 0) (> num 99))
	  (let ((rps (/ (float num) (float delta))))
	    (format t "~%rps:~A rows:~A delta:~A" rps num delta))))))

(defun cloudtrail-report-sync (path)
  (let ((cloudtrail-reports (or path "~/CT")))
    (walk-ct cloudtrail-reports
	     #'sync-ct-file)))

(defun cloudtrail-report-async (workers path)
  ;;(psql-create-tables)
  (let ((workers (parse-integer workers)))
    (setf (pcall:thread-pool-size) workers)
    (let ((cloudtrail-reports (or path "~/CT")))
      (walk-ct cloudtrail-reports
	       #'async-ct-file))
    (mapc #'pcall:join *mytasks*)))
