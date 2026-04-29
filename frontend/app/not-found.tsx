import Link from "next/link";

export default function NotFound() {
  return (
    <div className="error-page">
      <div className="error-code">404</div>
      <div className="error-title">Page Not Found</div>
      <p className="error-msg">That page does not exist.</p>
      <div className="error-actions">
        <Link href="/" className="btn">Go home</Link>
      </div>
    </div>
  );
}
