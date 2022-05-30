(in-package :metis)

(defvar *app* (make-instance 'ningle:<app>))
(defvar *server* "localhost")
(defvar *port* "5002")

(defun create-url (verb name)
  "Create a url given the verb root and name of items"
  (format nil "~~{<tr><td><a href=\"http://~a:~a/~a?~a=~~A\"> ~~:* ~~A </aref></td></tr>~~}" *server* *port* verb name))

(defun create-links (url items hdr foot)
  (concatenate 'string
	       hdr
	       (format nil url (sort items #'string-lessp))
	       foot))

(defun web/get-user-list ()
  (let ((users (metis::get-unique-values-list 'metis::username))
	(url (create-url "user-search" "users")))
    (create-links url users (web/pretty-header "users") (web/pretty-footer "users"))))

(defun web/get-region-list ()
  (let ((users (metis::get-unique-values-list 'metis::awsREgion)))
    (concatenate 'string
		 (web/pretty-header "Regions")
		 (format nil "~{<tr><td><a href=\"http://localhost:5002/region-search?region=~A\"> ~:* ~A </aref></td></tr>~}" (sort users #'string-lessp))
		 (web/pretty-footer "regions"))))

(defun web/get-useragent-list ()
  (let ((users (metis::get-unique-values-list 'metis::userAgent)))
    (concatenate 'string
		 (web/pretty-header "useragents")
		 (format nil "~{<tr><td><a href=\"http://localhost:5002/useragent-search?useragent=~A\"> ~:* ~A </aref></td></tr>~}" (sort users #'string-lessp))
		 (web/pretty-footer "useragents"))))

(defun web/get-event-list ()
  (let ((events (metis::get-unique-values-list 'metis::eventName)))
    (concatenate 'string
		 (web/pretty-header "events")
		 (format nil "~{<tr><td><a href=\"http://localhost:5002/event-search?event=~A\"> ~:* ~A </aref></td></tr>~}" (sort events #'string-lessp))
		 (web/pretty-footer "events"))))

(defun web/get-error-list ()
  (let ((errors (metis::get-unique-values-list 'metis::ErrorCode)))
    (concatenate 'string
		 (web/pretty-header "errors")
		 (format nil "~{<tr><td><a href=\"http://localhost:5002/error-search?error=~A\"> ~:* ~A </aref></td></tr>~}" (sort errors #'string-lessp))
		 (web/pretty-footer "errors"))))

(defun web/get-ip-list ()
  (let ((ips (metis::get-unique-values-list 'metis::sourceIPAddress)))
    (concatenate 'string
		 (web/pretty-header "ips")
		 (format nil "~{<tr><td><a href=\"http://localhost:5002/ip-search?ip=~A\"> ~:* ~A </aref></td></tr>~}" (sort ips #'string-lessp))
		 (web/pretty-footer "ips"))))

(defun web/pretty-header (name)
  (format nil "
<!DOCTYPE html>
<html>
<head>
<style>
#~A {
    font-family: \"Trebuchet MS\", Arial, Helvetica, sans-serif;
    border-collapse: collapse;
    width: 100%;
}

#~A td, #~A th {
    border: 1px solid #ddd;
    padding: 8px;
}

#~A tr:nth-child(even){background-color: #f2f2f2;}

#~A tr:hover {background-color: #ddd;}

#~A th {
    padding-top: 12px;
    padding-bottom: 12px;
    text-align: left;
    background-color: #4CAF50;
    color: white;
}
</style>
</head>
<body>
<table id=\"~A\">
"
	  name
	  name
	  name
	  name
	  name
	  name
	  name
	  ))

(defun web/pretty-footer (name)
  (format nil "</table></body></html>"))

(defun web/get-operations ()
  (concatenate 'string
	       (web/pretty-header "operations")
	       (format nil "~{<tr><td><a href=\"http://localhost:5002/~(~a~)\"> ~:* ~A </aref></td></tr>~}"
		       '(
			 :ips
			 :users
			 :events
			 :errors
			 :useragents
			 :regions
			 ))
	       (web/pretty-footer "operations")))

(defun web/search-user (user)
  (let* ((activity (metis::ct-get-by-klass-value-real 'metis::userName user)))
    (cl-ppcre:regex-replace-all "\\|"
				(concatenate 'string
					     (web/pretty-header "events")
					     (format nil "~{<th>~A</th>~}" '( :date :event :user :source :ip :userAgent :error :errorCode :userId))
					     (format nil "~{<tr>~A</tr>~}" (sort activity #'string-lessp))
					     (web/pretty-footer "events"))
				"<td>")))

(defun web/search-region (region)
  (let* ((activity (metis::ct-get-by-klass-value-real 'metis::awsRegion region)))
    (cl-ppcre:regex-replace-all "\\|"
				(concatenate 'string
					     (web/pretty-header "Regions")
					     (format nil "~{<th>~A</th>~}" '( :date :event :user :source :ip :userAgent :error :errorCode :userId))
					     (format nil "~{<tr>~A</tr>~}" (sort activity #'string-lessp))
					     (web/pretty-footer "regions"))
				"<td>")))


(defun web/search-useragent (useragent)
  (let* ((activity (metis::ct-get-by-klass-value-real 'metis::userAgent useragent)))
    (cl-ppcre:regex-replace-all "\\|"
				(concatenate 'string
					     (web/pretty-header "userAgents")
					     (format nil "~{<th>~A</th>~}" '( :date :event :user :source :ip :userAgent :error :errorCode :userId))
					     (format nil "~{<tr>~A</tr>~}" (sort activity #'string-lessp))
					     (web/pretty-footer "userAgents"))
				"<td>")))

(defun web/search-event (event)
  (let* ((activity
	  (metis::ct-get-by-klass-value-real 'metis::eventName event)))
    (cl-ppcre:regex-replace-all "\\|"
				(concatenate 'string
					     (web/pretty-header "events")
					     (format nil "~{<th>~A</th>~}" '( :date :event :user :source :ip :userAgent :error :errorCode :userId))
					     (format nil "~{<tr>~A</tr>~}" (sort activity #'string-lessp))
					     (web/pretty-footer "events"))
				"<td>")))

(defun web/search-ip (ip)
  (let* ((activity (metis::ct-get-by-klass-value-real 'metis::sourceIPAddress ip)))
    (cl-ppcre:regex-replace-all "\\|"
				(concatenate 'string
					     (web/pretty-header "ips")
					     (format nil "~{<th>~A</th>~}" '( :date :event :user :source :ip :userAgent :error :errorCode :userId))
					     (format nil "~{<tr>~A</tr>~}" (sort activity #'string-lessp))
					     (web/pretty-footer "ips"))
				"<td>")))


(defun web/search-error (err)
  (let* ((activity (metis::ct-get-by-klass-value-real 'metis::errorCode err)))
    (cl-ppcre:regex-replace-all "\\|"
				(concatenate 'string
					     (web/pretty-header "errors")
					     (format nil "~{<th>~A</th>~}" '( :date :event :user :source :ip :userAgent :error :errorCode :userId))
					     (format nil "~{<tr>~A</tr>~}" (sort activity #'string-lessp))
					     (web/pretty-footer "errors"))
				"<td>")))

(defun web/start ()
  (defvar output-sep "<td>")
  (metis::init-manardb)
  (setf (ningle:route *app* "/") `(200 (:content-type "text/html") (,(web/get-operations))))
  (setf (ningle:route *app* "/users") `(200 (:content-type "text/html") (,(web/get-user-list))))
  (setf (ningle:route *app* "/regions") `(200 (:content-type "text/html") (,(web/get-region-list))))
  (setf (ningle:route *app* "/useragents") `(200 (:content-type "text/html") (,(web/get-useragent-list))))
  (setf (ningle:route *app* "/events") `(200 (:content-type "text/html") (,(web/get-event-list))))
  (setf (ningle:route *app* "/errors") `(200 (:content-type "text/html") (,(web/get-error-list))))
  (setf (ningle:route *app* "/ips") `(200 (:content-type "text/html") (,(web/get-ip-list))))

  (setf (ningle:route *app* "/user-search")
	#'(lambda (params)
	    (let ((user (cdr (assoc "user" params :test #'string=))))
	      `(200 (:content-type "text/html") (,(web/search-user user))))))

  (setf (ningle:route *app* "/region-search")
	#'(lambda (params)
	    (let ((user (cdr (assoc "region" params :test #'string=))))
	      `(200 (:content-type "text/html") (,(web/search-region user))))))

    (setf (ningle:route *app* "/useragent-search")
	#'(lambda (params)
	    (let ((user (cdr (assoc "useragent" params :test #'string=))))
	      `(200 (:content-type "text/html") (,(web/search-useragent useragent))))))

  (setf (ningle:route *app* "/error-search")
	#'(lambda (params)
	    (let ((error (cdr (assoc "error" params :test #'string=))))
	      `(200 (:content-type "text/html") (,(web/search-error error))))))

  (setf (ningle:route *app* "/event-search")
	#'(lambda (params)
	    (let ((event (cdr (assoc "event" params :test #'string=))))
	      `(200 (:content-type "text/html") (,(web/search-event event))))))

  (setf (ningle:route *app* "/ip-search")
	#'(lambda (params)
	    (let ((ip (cdr (assoc "ip" params :test #'string=))))
	      `(200 (:content-type "text/html") (,(web/search-ip ip))))))

  ;;(clack:clackup *app* :port 5002 :server :hunchentoot)
  #+sbcl (loop (sleep 1))
  #+ccl (loop (sleep 1))
  )
