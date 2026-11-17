import { marked } from 'marked'
import DOMPurify from 'dompurify'

export function renderAgentMarkdown(text: string): string {
  const fragment = DOMPurify.sanitize(marked.parse(text, { async: false, breaks: true }), {
    ALLOWED_TAGS: ['p', 'br', 'strong', 'em', 'del', 'ul', 'ol', 'li', 'blockquote', 'code', 'pre', 'h2', 'h3', 'h4', 'table', 'thead', 'tbody', 'tr', 'th', 'td', 'a', 'hr'],
    ALLOWED_ATTR: ['href', 'title'], RETURN_DOM_FRAGMENT: true,
  })
  // Generated answers only link to known, read-only LocalPerks pages.
  fragment.querySelectorAll('a').forEach(link => {
    if (!/^\/(?:shops(?:\/\d+)?|vouchers(?:\?shopId=\d+)?|my-vouchers|orders\/\d+|agent-api\/agent\/knowledge\/sources\/[a-f0-9]{32})$/.test(link.getAttribute('href') || '')) link.removeAttribute('href')
  })
  const wrapper = document.createElement('div')
  wrapper.append(fragment)
  return wrapper.innerHTML
}
