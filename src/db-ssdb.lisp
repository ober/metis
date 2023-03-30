(in-package :metis)

;; fundamental metis ops
(defun ssdb/init ()
  (unless ssdb:*connection*
    (ssdb:connect)))

(defun ssdb/close ()
  (ssdb:disconnect))

(defun ssdb/have-we-seen-this-file (file)
  (ssdb:exists (format nil "F-~a" (get-filename-hash file))))

(defun ssdb/mark-file-processed (file)
  (ssdb:set (format nil "F-~a" (get-filename-hash file)) "1"))

(defun ssdb/normalize-insert (record)
  (destructuring-bind (
                       additionalEventData
                       apiVersion
                       awsRegion
                       errorCode
                       errorMessage
                       eventCategory
                       eventID
                       eventName
                       eventSource
                       eventTime
                       eventType
                       eventVersion
                       managementEvent
                       readOnly
                       recipientAccountId
                       requestID
                       requestParameters
                       resources
                       responseElements
                       serviceEventDetails
                       sessionCredentialFromConsole
                       sharedEventID
                       sourceIPAddress
                       tlsDetails
                       userAgent
                       userIdentity
                       userName
                       vpcEndpointId
                       )
      record
    (let ((epoch (rfc3339-to-epoch eventTime)))
      (ssdb:multi_hset (format nil "~a:~a" epoch requestID)
                       "aed" additionalEventData
                       "av" apiVersion
                       "ar" awsRegion
                       "ec" errorCode
                       "em" errorMessage
                       "eca" eventCategory
                       "en" eventName
                       "es" eventSource
                       ;;"ei" eventID
                       ;;"et" eventTime
                       "typ" eventType
                       "ev" eventVersion
                       "me" managementEvent
                       "ro" readOnly
                       "rai" recipientAccountId
                       "rqi" requestID
                       "rp" requestParameters
                       "res" resources
                       "re" responseElements
                       "sed" serviceEventDetails
                       "scf" sessionCredentialFromConsole
                       "sei" sharedEventID
                       "sia" sourceIPAddress
                       "td" tlsDetails
                       "ua" userAgent
                       "ui" userIdentity
                       "un" userName
                       "vpc" vpcEndpointId))))

;; ported kunabi style ops

(defun ssdb/fetch-print-hash (hit)
  (format t "|~a| ~{~a: ~a| ~}|~%"
          (epoch-to-rfc3339
           (parse-number:parse-number
            (car (cl-ppcre:split ":" hit))))
          (ssdb:multi_hget hit  "en" "un" "ui" "ua" "sia" "ec" "em" "sip" "ua" "es")))

(defun ssdb/db-key? (key)
  (ssdb:exists key))

(defun ssdb/db-get (key)
  (ssdb:get key))

(defun ssdb/get-stats ()
  (format t "~a" (ssdb:info)))

(defun ssdb/get-unique-region ()
  (ssdb/get-unique "ar"))

(defun ssdb/get-unique-ip ()
  (ssdb/get-unique "sia"))

(defun ssdb/get-unique-agent ()
  (ssdb/get-unique "ua"))

(defun ssdb/get-unique-names ()
  (ssdb/get-unique "un"))

(defun ssdb/get-unique-events ()
  (ssdb/get-unique "en"))

(defun ssdb/get-unique-errorcode ()
  (ssdb/get-unique "ec"))

(defun ssdb/get-unique (key)
  (let ((hits (sort-uniq (ssdb:qrange key 0 -1))))
    (mapcar
     (lambda (hit)
       (format t "~a~%" hit))
     hits)))

;; (defun ssdb/index (field)
;;   (let ((records (time (ssdb:hlist "" "" -1)))
;;         (seen '()))
;;     (format t "records: ~a~%" (length records))
;;     (mapcar
;;      (lambda (record)
;;        (unless (string= record "NIL")
;;          (let ((value (ssdb:hget record field)))
;;            (ssdb:qpush value record)
;;            (unless (member value seen :test #'string=)
;;              (progn
;;                (format t "not seen: ~a~%" value)
;;                (push value seen))))))
;;      records)
;;     (ssdb:qclear field)
;;     (ssdb/qpush-list field seen)))

(defun ssdb/count-calls ()
  (let* ((from (format nil "~a:" (epoch-one-day-ago)))
         (to (format nil "~a:" (epoch-now)))
         (records (ssdb:hlist from to -1)))
    (format t "records: ~a~%" (length records))
    (mapcar
      (lambda (record)
        (let ((un (ssdb:hget record "un"))
              (en (ssdb:hget record "en")))
            (ssdb:zincr un en 1)))
        records)))

(defun ssdb/count-by-user (user)
  (let ((ops (ssdb:zkeys user "" "" "" -1)))
    (format t "Total: ~a~%" (ssdb:zsum user "" ""))
    (mapcar
     (lambda (op)
       (format t "|~a| ~a|~%" op (ssdb:zget user op)))
     ops)))

(defun ssdb/totals ()
  (let ((users (ssdb:zlist "" "" -1)))
    (mapcar
     (lambda (user)
       (format t "|~a | ~a|~%" user (ssdb:zsum user "" "")))
     users)))



(defun ssdb/index (field)
  (let* ((from (format nil "~a:" (epoch-one-day-ago)))
         (to (format nil "~a:" (epoch-now)))
         (records (ssdb:hlist from to -1))
         (seen '()))
    (format t "records: ~a~%" (length records))
    (mapcar
     (lambda (record)
       (unless (string= record "NIL")
         (let ((value (ssdb:hget record field)))
           (ssdb:qpush value record)
           (unless (member value seen :test #'string=)
             (progn
               (format t "not seen: ~a~%" value)
               (push value seen))))))
     records)
    (ssdb:qclear field)
    (ssdb/qpush-list field seen)))

(defun ssdb/qpush-list (field list)
  (mapcar
   (lambda (item)
     (ssdb:qpush field item))
   (sort-uniq list)))

(defun ssdb/get-by-index (key)
  "Get all records from index of key"
  (let ((hits (sort-uniq (ssdb:qrange key 0 -1))))
    (mapcar
     (lambda (hit)
       (ssdb/fetch-print-hash hit))
     hits)))

(defun ssdb/unique-queue (q)
  (let* ((items (ssdb:qrange q 0 -1))
         (uniqs (sort-uniq items)))
    (format t "q: ~a orig: ~a uniq: ~a~%" q (length items) (length uniqs))
    (ssdb:qclear q)
    (mapcar
     (lambda (uniq)
       (ssdb:qpush q uniq))
     uniqs)))

;; (defun ssdb/uniq-queues ()
;;   "For each queue, go fetch its contents uniq them and put them back"
;;   (let ((queues (ssdb:qlist "" "" -1)))
;;     (format t "queues: ~A~%" (length queues))
;;     (mapcar
;;      (lambda (q)
;;        (let* ((items (ssdb:qrange q 0 -1))
;;               (uniqs (sort-uniq items)))
;;          (format t "q:~a size:~a uniq:~a~%" q (length items) (length uniqs))
;;          (ssdb:qclear q)
;;          (ssdb:qpush q uniqs)))
;;      queues)))
