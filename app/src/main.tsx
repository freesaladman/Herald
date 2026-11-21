import React, { useEffect, useState } from "react";
import ReactDOM from "react-dom/client";
import "./App.css";
import { Pane } from "./pane/Pane";
import { Toast } from "./toast/Toast";

type Route = "toast" | "pane";

function currentRoute(): Route {
  return location.hash === "#/toast" ? "toast" : "pane";
}

function Router() {
  const [route, setRoute] = useState<Route>(currentRoute);

  useEffect(() => {
    const onChange = () => setRoute(currentRoute());
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);

  useEffect(() => {
    document.body.dataset.route = route;
  }, [route]);

  return route === "toast" ? <Toast /> : <Pane />;
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <Router />
  </React.StrictMode>,
);
