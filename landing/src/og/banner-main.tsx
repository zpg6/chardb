import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { Banner } from "./Banner";
import "../index.css";

const rootElement = document.getElementById("root");
if (!rootElement) throw new Error("Missing #root element");

createRoot(rootElement).render(
    <StrictMode>
        <Banner />
    </StrictMode>
);
