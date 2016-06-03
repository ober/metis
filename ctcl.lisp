(in-package :metis)

(defvar *mytasks* (list))

(defun have-we-seen-this-file (file)
  (format t ".")
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
      (format t "N")
      ;;(format t "New:~A~%" (file-namestring x))
      (parse-ct-contents x))))

;; (defun parse-ct-contents (x)
;;   (let ((records (cdr (elt (read-json-gzip-file x) 0))))
;;     (dolist (x records)
;;       (let (
;; 	    (event-version (cdr-assoc :event-version x))
;; 	    (user-identity (cdr-assoc :user-identity x))
;; 	    (event-time (cdr-assoc :event-time x))
;; 	    (event-source (cdr-assoc :event-source x))
;; 	    (event-name (cdr-assoc :event-name x))
;; 	    (aws-region (cdr-assoc :aws-region x))
;; 	    (source-ip-address (cdr-assoc :source-+ip+-address x))
;; 	    (user-agent (cdr-assoc :user-agent x))
;; 	    (request-parameters (cdr-assoc :request-parameters x))
;; 	    (response-elements (cdr-assoc :response-elements x))
;; 	    (request-id (cdr-assoc :request-+id+ x))
;; 	    (event-id (cdr-assoc :event-+id+ x))
;; 	    (event-type (cdr-assoc :event-type x))
;; 	    )
;; 	(enqueue (list event-version user-identity event-time event-source event-name aws-region source-ip-address user-agent request-parameters response-elements request-id event-id event-type) *q*)
;; 	;;(make-event event-version user-identity event-time event-source event-name aws-region source-ip-address user-agent request-parameters response-elements request-id event-id event-type)
;; 	))))

;;	(enqueue (list event-version user-identity event-time event-source aws-region source-ip-address user-agent request-parameters response-elements request-id event-id event-type) *q*)))))

;; ;; ((:event-version . "1.02") (:user-identity (:type . "IAMUser") (:principal-id . "AIDAJFYJ3F2NIWDWYTHC4") (:arn . "arn:aws:iam::140809180094:user/sns-manager") (:account-id . "140809180094") (:access-key-id . "AKIAJGYRKI53P3AXG7AQ") (:user-name . "sns-manager")) (:event-time . "2016-01-01T13:00:24Z") (:event-source . "sns.amazonaws.com") (:event-name . "CreatePlatformEndpoint") (:aws-region . "us-west-2") (:source-+ip+-address . "52.35.51.205") (:user-agent . "aws-sdk-java/1.9.18 Linux/3.2.0-54-virtual Java_HotSpot(TM)_64-Bit_Server_VM/24.80-b11/1.7.0_80") (:request-parameters (:platform-application-arn . "arn:aws:sns:us-west-2:140809180094:app/APNS/TargetCAP") (:token . "REDACTED")) (:response-elements (:endpoint-arn . "arn:aws:sns:us-west-2:140809180094:endpoint/APNS/TargetCAP/adc1f986-3e4a-3b92-b0e0-c14675c296ff")) (:request-+id+ . "af8e499b-1a35-5e1d-b870-069b609d13f4") (:event-+id+ . "706916c8-5d08-45fb-bffa-7b579aa7c6ef") (:event-type . "AwsApiCall") (:recipient-account-id . "140809180094"))

;; (defun parse-ct-contents (x)
;;   (enqueue (cdr (elt (read-json-gzip-file x) 0)) *q*))

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
  ;;(psql-create-tables)
  (let ((workers (parse-integer workers)))
    (setf (pcall:thread-pool-size) workers)
    (let ((cloudtrail-reports (or path "~/CT")))
      (walk-ct cloudtrail-reports
	       #'async-ct-file))
    ;;    (ignore-errors
    (mapc #'pcall:join *mytasks*)))
;;)
