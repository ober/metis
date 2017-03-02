(ql:quickload '(:clack :ningle :metis))

(defvar *app* (make-instance 'ningle:<app>))
(metis::init-manardb)

(setf (ningle:route *app* "/") "Welcome to metis")
(setf (ningle:route *app* "/users") `(200 (:content-type "text/html") ,(metis::get-unique-values-list-html 'metis::username)))

(clack:clackup *app* :port 5002 :server :hunchentoot)
